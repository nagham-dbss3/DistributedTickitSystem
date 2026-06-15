import threading
import tkinter as tk
from tkinter import ttk
import random
from datetime import datetime
import time

from circuit_breaker.state_machine import CircuitOpenError
from ingress import BookingIngressProxy
from locking import DistributedLeaseManager, LeaseConflictError, LeaseNotFoundError
from load_balancer import (
    LoadBalancer,
    ServerMetrics,
    RoundRobinStrategy,
    WeightedRoundRobinStrategy,
    ResourceAwareLeastConnectionsStrategy,
    ConsistentHashingStrategy,
    AdaptiveFeedbackStrategy,
)
from monitoring import HeartbeatMonitor, FailureDetector
from circuit_breaker import Breaker, CircuitState
from failover.failover_manager import FailoverManager
from failover.recovery_manager import RecoveryManager
from retry.retry_manager import RetryManager
from synchronization import EventualConsistencySimulator, StateReplicator, StateStore
from telemetry.metrics_collector import MetricsCollector


class TransientBookingError(Exception):
    """Retryable booking failure caused by transient server behavior."""


class NonRetryableBookingError(Exception):
    """Booking failure that should not be retried."""


class SeatButton(ttk.Button):
    STATES = ("available", "locked", "reserved")

    def __init__(self, master, seat_id, gateway, on_change, lease_manager=None, **kwargs):
        # Clicking submits through HTTP ingress; a real lease backs the visible lock.
        super().__init__(master, command=self.toggle, **kwargs)
        self.seat_id = seat_id
        self.gateway = gateway
        self.on_change = on_change
        self.lease_manager = lease_manager
        self.lease_id = None
        self.state = "available"
        self.owner = None
        self.remaining = 0
        self._after_id = None
        self._lease_after_id = None
        # style names for ttk should end with the widget class suffix, e.g. '.TButton'
        self.style_name = f"Seat{seat_id}.TButton"
        self._setup_style()
        self._update_appearance()

    def _setup_style(self):
        s = ttk.Style()
        # ensure base styles exist; colors will be set when state changes
        s.configure(self.style_name, background="#8BC34A")

    def _update_appearance(self):
        # Update button text and style based on state
        if self.state == "available":
            txt = str(self.seat_id)
            bg = "#8BC34A"
            state = "normal"
        elif self.state == "locked":
            owner = self.owner or "-"
            txt = f"{self.seat_id}\n{owner}\n{self.remaining}s"
            bg = "#FFEB3B"
            state = "normal"
        else:  # reserved
            txt = f"{self.seat_id}\nRESERVED"
            bg = "#F44336"
            state = "disabled"

        s = ttk.Style()
        s.configure(self.style_name, background=bg)
        try:
            self.configure(text=txt, style=self.style_name, state=state)
        except Exception:
            # some platforms may not accept style changes on the fly
            self.configure(text=txt, state=state)

    def toggle(self):
        # Start a booking via the API Gateway if available
        if self.state != "available":
            return
        if self.gateway:
            self.gateway.submit_booking(self.seat_id, requester="You")
        else:
            # fallback to direct lock
            self.lock(owner="You", duration=30)

    def lock(self, owner="You", duration=30, lease_id=None):
        # begin lock countdown backed by the active distributed lease
        self.owner = owner
        self.lease_id = lease_id
        self.remaining = duration
        self.state = "locked"
        self._update_appearance()
        self._schedule_tick()
        self._schedule_lease_renewal()
        self.on_change(self.seat_id, "locked")

    def _schedule_tick(self):
        # schedule one-second updates
        if self._after_id:
            try:
                self.after_cancel(self._after_id)
            except Exception:
                pass
        self._after_id = self.after(1000, self._tick)

    def _tick(self):
        self.remaining -= 1
        if self.lease_manager and self.lease_id:
            try:
                self.lease_manager.validate(self.lease_id, seat_id=self.seat_id)
            except LeaseNotFoundError:
                self._after_id = None
                self.cancel_lock()
                self.on_change(self.seat_id, "available")
                return
        if self.remaining <= 0:
            self._after_id = None
            self.confirm_reservation()
            return
        self._update_appearance()
        self._schedule_tick()

    def _schedule_lease_renewal(self):
        if not self.lease_manager or not self.lease_id:
            return
        if self._lease_after_id:
            try:
                self.after_cancel(self._lease_after_id)
            except Exception:
                pass
        self._lease_after_id = self.after(5000, self._renew_lease)

    def _renew_lease(self):
        if self.state != "locked" or not self.lease_manager or not self.lease_id:
            self._lease_after_id = None
            return
        try:
            self.lease_manager.renew(self.lease_id)
            self._schedule_lease_renewal()
        except LeaseNotFoundError:
            self._lease_after_id = None
            self.cancel_lock()
            self.on_change(self.seat_id, "available")

    def confirm_reservation(self):
        # convert locked -> reserved
        if self.lease_manager and self.lease_id:
            try:
                self.lease_manager.validate(self.lease_id, seat_id=self.seat_id)
            except LeaseNotFoundError:
                self.cancel_lock()
                self.on_change(self.seat_id, "available")
                return
            self.lease_manager.release(self.lease_id)
        self.state = "reserved"
        self._after_id = None
        self._lease_after_id = None
        self.lease_id = None
        self._update_appearance()
        self.on_change(self.seat_id, "reserved")

    def cancel_lock(self):
        # cancel any running lock and return to available
        if self._after_id:
            try:
                self.after_cancel(self._after_id)
            except Exception:
                pass
            self._after_id = None
        if self._lease_after_id:
            try:
                self.after_cancel(self._lease_after_id)
            except Exception:
                pass
            self._lease_after_id = None
        if self.lease_manager and self.lease_id:
            self.lease_manager.release(self.lease_id)
            self.lease_id = None
        self.owner = None
        self.remaining = 0
        self.state = "available"
        self._update_appearance()


class ServerStatus(ttk.Frame):
    def __init__(self, master, name, **kwargs):
        super().__init__(master, **kwargs)
        self.name = name
        self.status = "online"
        # heartbeat support (will be set by Dashboard after creation)
        self.hb_monitor = None
        self._hb_after_id = None
        # simulated metrics
        self.cpu = round(random.uniform(2.0, 12.0), 1)
        self.connections = random.randint(0, 8)
        self.requests = 0
        self._sim_id = None
        self._build()
        self.start_simulation()

        # circuit breaker placeholder
        self.breaker = None

    def _build(self):
        self.lbl = ttk.Label(self, text=self.name)
        self.lbl.grid(row=0, column=0, sticky="w")
        self.canvas = tk.Canvas(self, width=18, height=18, highlightthickness=0)
        self.oval = self.canvas.create_oval(2, 2, 16, 16, fill="#4CAF50")
        self.canvas.grid(row=0, column=1, padx=6)
        self.toggle_btn = ttk.Button(self, text="Toggle", command=self.toggle)
        self.toggle_btn.grid(row=0, column=2, padx=6)
        # Metrics labels
        self.cpu_var = tk.StringVar(value=f"CPU: {self.cpu}%")
        self.conn_var = tk.StringVar(value=f"Conns: {self.connections}")
        self.req_var = tk.StringVar(value=f"Reqs: {self.requests}")
        ttk.Label(self, textvariable=self.cpu_var).grid(row=1, column=0, columnspan=3, sticky="w", pady=(4,0))
        ttk.Label(self, textvariable=self.conn_var).grid(row=2, column=0, columnspan=3, sticky="w")
        ttk.Label(self, textvariable=self.req_var).grid(row=3, column=0, columnspan=3, sticky="w")

        # Circuit breaker UI variables
        self.breaker_state_var = tk.StringVar(value="-")
        self.breaker_fail_var = tk.IntVar(value=0)
        self.breaker_rate_var = tk.DoubleVar(value=0.0)
        self.breaker_cd_var = tk.StringVar(value='-')

        ttk.Label(self, text="CB State:").grid(row=4, column=0, sticky="w")
        ttk.Label(self, textvariable=self.breaker_state_var).grid(row=4, column=1, sticky="w")
        ttk.Label(self, text="Failures:").grid(row=5, column=0, sticky="w")
        ttk.Label(self, textvariable=self.breaker_fail_var).grid(row=5, column=1, sticky="w")
        ttk.Label(self, text="Error %:").grid(row=6, column=0, sticky="w")
        ttk.Label(self, textvariable=self.breaker_rate_var).grid(row=6, column=1, sticky="w")
        ttk.Label(self, text="Cooldown:").grid(row=7, column=0, sticky="w")
        ttk.Label(self, textvariable=self.breaker_cd_var).grid(row=7, column=1, sticky="w")

    def toggle(self):
        self.status = "offline" if self.status == "online" else "online"
        color = "#F44336" if self.status == "offline" else "#4CAF50"
        self.canvas.itemconfig(self.oval, fill=color)
        # start/stop simulation when toggled
        if self.status == "online":
            self.start_simulation()
            # resume heartbeat if monitor available
            if getattr(self, 'hb_monitor', None):
                self.start_heartbeat()
        else:
            self.stop_simulation()
            # stop heartbeat when offline
            self.stop_heartbeat()
        # update display values immediately
        self._update_metric_vars()

    # Heartbeat sender for this simulated server
    def start_heartbeat(self, interval: int = 5):
        if self._hb_after_id:
            return
        # send immediate heartbeat then schedule recurring
        self._send_heartbeat()
        self._hb_after_id = self.after(interval * 1000, self._heartbeat_tick, interval)

    def _heartbeat_tick(self, interval: int):
        self._send_heartbeat()
        self._hb_after_id = self.after(interval * 1000, self._heartbeat_tick, interval)

    def stop_heartbeat(self):
        if self._hb_after_id:
            try:
                self.after_cancel(self._hb_after_id)
            except Exception:
                pass
            self._hb_after_id = None

    def _send_heartbeat(self):
        # Only send heartbeat when online and monitor is set
        if self.status != 'online':
            return
        if getattr(self, 'hb_monitor', None):
            try:
                self.hb_monitor.record_heartbeat(self.name)
            except Exception:
                pass

    def _update_metric_vars(self):
        self.cpu_var.set(f"CPU: {self.cpu}%")
        self.conn_var.set(f"Conns: {self.connections}")
        self.req_var.set(f"Reqs: {self.requests}")

    def start_simulation(self):
        if self._sim_id:
            return
        # schedule first tick
        self._sim_id = self.after(1000, self._simulate_tick)

    def set_breaker(self, breaker: Breaker):
        self.breaker = breaker
        # initialize UI
        try:
            st = self.breaker.get_state().value
        except Exception:
            st = '-'
        self.breaker_state_var.set(st)

    def stop_simulation(self):
        if self._sim_id:
            try:
                self.after_cancel(self._sim_id)
            except Exception:
                pass
            self._sim_id = None

    def _simulate_tick(self):
        # called every second to update simulated metrics
        if self.status == "online":
            # CPU fluctuates a bit
            self.cpu = round(max(0.0, min(100.0, self.cpu + random.uniform(-3.0, 6.0))), 1)
            # connections fluctuate
            self.connections = max(0, self.connections + random.randint(-2, 3))
            # requests increase randomly
            self.requests += random.randint(0, 8)
        else:
            # drift down when offline
            self.cpu = round(max(0.0, self.cpu - random.uniform(0.5, 2.0)), 1)
            self.connections = max(0, self.connections - random.randint(0, 2))
        self._update_metric_vars()
        # schedule next tick
        self._sim_id = self.after(1000, self._simulate_tick)


class APIGateway:
    def __init__(self, dashboard, parent_frame, servers):
        self.dashboard = dashboard
        self.parent = parent_frame
        self.servers = servers
        
        # Initialize with RoundRobinStrategy
        self._current_strategy_name = "Round Robin"
        self._strategies = {
            "Round Robin": RoundRobinStrategy(),
            "Weighted Round Robin": WeightedRoundRobinStrategy(weights={"Server 1": 5, "Server 2": 3, "Server 3": 1}),
            "Resource-Aware": ResourceAwareLeastConnectionsStrategy(),
            "Consistent Hashing": ConsistentHashingStrategy(vnode_count=50),
            "Adaptive Feedback": AdaptiveFeedbackStrategy(),
        }
        self.load_balancer = LoadBalancer(self._strategies[self._current_strategy_name])
        self._last_selected_server = None
        self._last_routing_reason = ""
        self.failure_injection_rate = 0.25
        self.ingress_proxy = BookingIngressProxy(self.handle_ingress_booking)
        self._build_ui()

    def start_ingress(self):
        self.ingress_proxy.start()

    def stop_ingress(self):
        self.ingress_proxy.stop()

    def _build_ui(self):
        gf = ttk.LabelFrame(self.parent, text="API Gateway", style="Panel.TLabelframe")
        gf.grid(row=0, column=0, sticky="we", pady=(0, 6))
        
        ttk.Label(gf, text="Strategy:").grid(row=0, column=0, sticky="w")
        self.strategy_var = tk.StringVar(value=self._current_strategy_name)
        strategy_combo = ttk.Combobox(gf, textvariable=self.strategy_var, 
                                       values=list(self._strategies.keys()), state="readonly", width=20)
        strategy_combo.grid(row=0, column=1, sticky="ew", padx=4)
        strategy_combo.bind("<<ComboboxSelected>>", self._on_strategy_changed)
        
        ttk.Label(gf, text="Selected:").grid(row=1, column=0, sticky="w")
        self.selected_var = tk.StringVar(value="-")
        ttk.Label(gf, textvariable=self.selected_var).grid(row=1, column=1, sticky="w")
        
        ttk.Label(gf, text="Reason:").grid(row=2, column=0, sticky="w")
        self.reason_var = tk.StringVar(value="-")
        ttk.Label(gf, textvariable=self.reason_var, wraplength=200).grid(row=2, column=1, sticky="w")

    def _on_strategy_changed(self, event=None):
        new_strategy_name = self.strategy_var.get()
        if new_strategy_name != self._current_strategy_name:
            self._current_strategy_name = new_strategy_name
            self.load_balancer.set_strategy(self._strategies[new_strategy_name])
            try:
                self.dashboard.strategy_var.set(new_strategy_name)
                self.dashboard._highlight_routing_path()
            except Exception:
                pass
            self.dashboard.append_log(f"Gateway switched to {new_strategy_name} strategy")

    def submit_booking(self, seat_id, requester="You"):
        """Submit a GUI booking through HTTP ingress with retry/backoff."""
        thread = threading.Thread(
            target=self._submit_booking_with_retry,
            args=(seat_id, requester),
            daemon=True,
        )
        thread.start()

    def _submit_booking_with_retry(self, seat_id, requester):
        try:
            self.dashboard.booking_retry_manager.call(
                lambda: self._submit_booking_once(seat_id, requester)
            )
            self.dashboard.after(0, lambda: self.dashboard.retry_status_var.set("Succeeded"))
        except NonRetryableBookingError as exc:
            self.dashboard.after(0, lambda: self.dashboard.retry_status_var.set("Blocked"))
            self.dashboard.after(0, lambda: self.dashboard.append_log(f"Booking not retried: {exc}"))
        except Exception as exc:
            self.dashboard.after(0, lambda: self.dashboard.retry_status_var.set("Failed"))
            self.dashboard.after(0, lambda: self.dashboard.append_log(f"Booking failed after retries: {exc}"))

    def _submit_booking_once(self, seat_id, requester):
        result = self.ingress_proxy.submit_booking(seat_id, requester)
        if result.get("ok"):
            return result

        error = result.get("error", "booking_failed")
        message = result.get("message", error)
        if error == "transient_failure":
            raise TransientBookingError(message)
        raise NonRetryableBookingError(message)

    def handle_ingress_booking(self, payload):
        """HTTP ingress callback; marshal booking work onto the Tk thread."""
        try:
            seat_id = int(payload.get("seat_id"))
        except Exception:
            return {"ok": False, "error": "bad_request", "message": "seat_id is required"}
        requester = str(payload.get("requester") or "You")

        completed = threading.Event()
        result_holder = {}

        def run_on_ui_thread():
            try:
                result_holder["result"] = self.route_booking(seat_id, requester=requester)
            except Exception as exc:
                result_holder["result"] = {"ok": False, "error": "gateway_error", "message": str(exc)}
            finally:
                completed.set()

        self.dashboard.after(0, run_on_ui_thread)
        if not completed.wait(timeout=5.0):
            return {"ok": False, "error": "gateway_timeout", "message": "Gateway did not respond in time"}
        return result_holder["result"]

    def _build_server_metrics(self):
        """Convert ServerStatus simulated metrics to ServerMetrics objects."""
        metrics = []
        for srv in self.servers:
            # error_rate and response_time are simulated for now
            error_rate = random.uniform(0.0, 0.05)
            response_time = random.uniform(50.0, 500.0)
            
            m = ServerMetrics(
                server_id=srv.name,
                cpu_usage=srv.cpu,
                active_connections=srv.connections,
                response_time=response_time,
                error_rate=error_rate,
                request_count=srv.requests,
                status=srv.status,
            )
            metrics.append(m)
            self.dashboard.telemetry_collector.ingest(m)
        return metrics

    def _score_calculation(self, metrics, selected_server_id, routing_key):
        """Build a human-readable explanation for the selected strategy."""
        online = [m for m in metrics if m.status == "online"]
        if not online:
            return "No online servers available."

        if self._current_strategy_name == "Round Robin":
            order = " -> ".join(m.server_id for m in online)
            return f"Round-robin order among online servers: {order}; key {routing_key} is not used by this algorithm."

        if self._current_strategy_name == "Weighted Round Robin":
            strategy = self._strategies[self._current_strategy_name]
            weights = []
            for metric in online:
                weight = strategy._weight_for(metric.server_id)
                weights.append(f"{metric.server_id}=weight {weight}")
            return "Smooth weighted round-robin using " + ", ".join(weights) + "."

        if self._current_strategy_name == "Resource-Aware":
            strategy = self._strategies[self._current_strategy_name]
            scores = []
            for metric in online:
                score = strategy._compute_score(metric)
                scores.append(
                    f"{metric.server_id}: ({metric.active_connections}*{strategy.w_conn}) + "
                    f"({metric.cpu_usage:.1f}*{strategy.w_cpu}) + "
                    f"({metric.response_time:.0f}*{strategy.w_resp}) = {score:.2f}"
                )
            return "Lowest score wins. " + "; ".join(scores)

        if self._current_strategy_name == "Consistent Hashing":
            strategy = self._strategies[self._current_strategy_name]
            key_hash = strategy._hash(routing_key)
            return f"Routing key '{routing_key}' hashes to {hex(key_hash)} and maps to {selected_server_id} on the hash ring."

        if self._current_strategy_name == "Adaptive Feedback":
            strategy = self._strategies[self._current_strategy_name]
            scores = []
            for metric in online:
                score = strategy._health_score(metric)
                scores.append(
                    f"{metric.server_id}: 100 - CPU {metric.cpu_usage:.1f} - "
                    f"Conn {metric.active_connections}*2 - Resp {metric.response_time:.0f}*0.5 - "
                    f"Err {metric.error_rate:.2%}*5 = {score:.2f}"
                )
            return "Highest health score wins. " + "; ".join(scores)

        return f"Selected by {self._current_strategy_name}."

    def route_booking(self, seat_id, requester="You"):
        started_at = time.time()
        self.dashboard.record_request()
        self.dashboard.append_log(f"Ingress proxy forwarded booking request for Seat {seat_id} from {requester}")
        
        # Build current metrics from servers
        metrics = self._build_server_metrics()
        
        # Select server using current strategy
        routing_key = f"seat-{seat_id}-{requester}"
        selected_server_id = self.load_balancer.select(metrics, routing_key=routing_key)
        
        if selected_server_id is None:
            self.dashboard.append_log(f"Gateway: No online servers available for Seat {seat_id}")
            self.dashboard.set_active_route(None)
            self.dashboard.record_failure()
            self.dashboard.telemetry_collector.record_event(
                "failure",
                f"No online servers for Seat {seat_id}",
                latency_ms=(time.time() - started_at) * 1000,
            )
            return {"ok": False, "error": "no_servers", "message": "No online servers available"}

        self._last_selected_server = selected_server_id
        score_calculation = self._score_calculation(metrics, selected_server_id, routing_key)
        self._last_routing_reason = f"by {self._current_strategy_name} strategy using routing key {routing_key}"
        self.selected_var.set(selected_server_id)
        self.reason_var.set(self._last_routing_reason)
        try:
            self.dashboard.selected_server_var.set(selected_server_id)
            self.dashboard.reason_var.set(self._last_routing_reason)
            self.dashboard.update_decision_details(
                algorithm=self._current_strategy_name,
                selected_server=selected_server_id,
                routing_key=routing_key,
                routing_reason=self._last_routing_reason,
                score_calculation=score_calculation,
            )
        except Exception:
            pass

        # Find the server object
        srv = None
        for s in self.servers:
            if s.name == selected_server_id:
                srv = s
                break

        # If selected server is unavailable, fail over to a healthy replacement
        if srv is None or srv.status != "online":
            offline_name = selected_server_id
            healthy_ids = [m.server_id for m in metrics if m.status == "online" and m.server_id != offline_name]
            if healthy_ids:
                replacement = healthy_ids[0]
                self.dashboard.append_log("[FAILOVER]")
                self.dashboard.append_log(f"Server {offline_name} unavailable")
                self.dashboard.append_log(f"Redirecting traffic to {replacement}")
                selected_server_id = replacement
                self._last_selected_server = selected_server_id
                self.selected_var.set(selected_server_id)
                self._last_routing_reason = f"failover from {offline_name}"
                self.reason_var.set(self._last_routing_reason)
                try:
                    self.dashboard.selected_server_var.set(selected_server_id)
                    self.dashboard.reason_var.set(self._last_routing_reason)
                    self.dashboard.update_decision_details(
                        algorithm=self._current_strategy_name,
                        selected_server=selected_server_id,
                        routing_key=routing_key,
                        routing_reason=self._last_routing_reason,
                        score_calculation=f"Original selection {offline_name} was unavailable; failover selected first healthy server {selected_server_id}.",
                    )
                except Exception:
                    pass
                for s in self.servers:
                    if s.name == selected_server_id:
                        srv = s
                        break
            else:
                self.dashboard.append_log(f"Gateway: No healthy failover servers available for Seat {seat_id}")
                self.dashboard.set_active_route(None)
                self.dashboard.record_failure()
                self.dashboard.telemetry_collector.record_event(
                    "failure",
                    f"No healthy failover servers for Seat {seat_id}",
                    latency_ms=(time.time() - started_at) * 1000,
                )
                return {"ok": False, "error": "no_servers", "message": "No healthy failover servers available"}

        if srv is None:
            self.dashboard.append_log(f"Gateway: Could not find server {selected_server_id}")
            self.dashboard.set_active_route(None)
            self.dashboard.record_failure()
            self.dashboard.telemetry_collector.record_event(
                "failure",
                f"Selected server {selected_server_id} not found for Seat {seat_id}",
                server_id=selected_server_id,
                latency_ms=(time.time() - started_at) * 1000,
            )
            return {"ok": False, "error": "server_not_found", "message": f"Could not find server {selected_server_id}"}

        breaker = getattr(srv, "breaker", None)
        try:
            if breaker and not breaker.allow_request():
                self.dashboard.append_log(f"Gateway: Circuit breaker OPEN for {selected_server_id}; request rejected")
                self.dashboard.set_active_route(None)
                self.dashboard.record_failure()
                self.dashboard.telemetry_collector.record_event(
                    "circuit_open",
                    f"Circuit breaker blocked Seat {seat_id}",
                    server_id=selected_server_id,
                    latency_ms=(time.time() - started_at) * 1000,
                    circuit_state=breaker.get_state().value,
                )
                return {
                    "ok": False,
                    "error": "circuit_open",
                    "message": f"Circuit breaker OPEN for {selected_server_id}",
                }
        except Exception:
            pass

        circuit_state = breaker.get_state().value if breaker else "UNKNOWN"
        if random.random() < self.failure_injection_rate:
            if breaker:
                breaker.record_failure()
                circuit_state = breaker.get_state().value
            self.dashboard.record_failure()
            self.dashboard.append_log(
                f"{selected_server_id} transient booking failure for Seat {seat_id}; "
                f"CB={circuit_state}"
            )
            self.dashboard.telemetry_collector.record_event(
                "failure",
                f"Injected transient booking failure for Seat {seat_id}",
                server_id=selected_server_id,
                latency_ms=(time.time() - started_at) * 1000,
                circuit_state=circuit_state,
            )
            return {
                "ok": False,
                "error": "transient_failure",
                "message": f"{selected_server_id} transient booking failure",
            }

        try:
            lease = self.dashboard.lease_manager.acquire(
                seat_id,
                holder=selected_server_id,
                owner=requester,
                ttl_seconds=30,
            )
        except LeaseConflictError as exc:
            self.dashboard.record_failure()
            self.dashboard.append_log(f"Lease denied for Seat {seat_id}: {exc}")
            self.dashboard.telemetry_collector.record_event(
                "lease_conflict",
                str(exc),
                server_id=selected_server_id,
                latency_ms=(time.time() - started_at) * 1000,
                circuit_state=circuit_state,
            )
            return {"ok": False, "error": "lease_conflict", "message": str(exc)}

        self.dashboard.set_active_route(selected_server_id)
        
        # Increment connections and requests
        srv.connections += 1
        srv.requests += 1
        srv._update_metric_vars()
        self.dashboard.append_log(
            f"Gateway routed Seat {seat_id} -> {selected_server_id} "
            f"({self._current_strategy_name}, routing_key={routing_key})"
        )
        self.dashboard.telemetry_collector.record_event(
            "routing",
            f"Seat {seat_id} routed to {selected_server_id} with key {routing_key}",
            server_id=selected_server_id,
            latency_ms=(time.time() - started_at) * 1000,
            circuit_state=circuit_state,
        )

        # Simulate network/processing delay then confirm lock
        delay = random.randint(150, 700)
        def _confirm():
            srv.connections = max(0, srv.connections - 1)
            srv._update_metric_vars()
            self.dashboard.append_log(f"{selected_server_id} accepted booking for Seat {seat_id}")
            try:
                if breaker:
                    breaker.record_success()
            except Exception:
                pass
            self.dashboard.record_success()
            seat_btn = self.dashboard.seat_buttons.get(seat_id)
            if seat_btn:
                seat_btn.lock(owner=requester, duration=30, lease_id=lease.lease_id)

        self.dashboard.after(delay, _confirm)
        return {
            "ok": True,
            "server": selected_server_id,
            "lease_id": lease.lease_id,
            "routing_key": routing_key,
        }


class Dashboard(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Distributed Ticket Booking System - Dashboard")
        self.geometry("1600x900")
        self.minsize(1100, 720)
        self._create_styles()
        # Heartbeat monitor and failure detector
        self.hb_monitor = HeartbeatMonitor(heartbeat_interval_sec=5.0, timeout_sec=15.0)
        self.failure_detector = FailureDetector(self.hb_monitor, check_interval=1.0, logger=print)
        self.gateway = None  # Will be created after servers
        self.request_counter = 0
        self.success_count = 0
        self.failure_count = 0
        self._request_events = []
        self.last_decision_details = {}
        self.retry_manager = None
        self.retry_attempts = 0
        self.retry_delay = 0.0
        self.backoff_stage = 0
        self.lease_manager = DistributedLeaseManager(default_ttl_seconds=30.0)
        self.telemetry_collector = MetricsCollector()
        self.failover_manager = FailoverManager(logger=self.append_log)
        self.recovery_manager = RecoveryManager(logger=self.append_log)
        self.booking_retry_manager = None
        self._pending_failover_servers = set()
        self._active_failovers = set()
        self._active_route_server = None
        self._topology_nodes = {}
        self._topology_edges = {}
        self._topology_labels = {}
        self.primary_server_var = tk.StringVar(value="-")
        self.active_failovers_var = tk.StringVar(value="None")
        self.recovery_event_history = []
        self.state_stores = {}
        self.sync_replicators = {}
        self.consistency_simulator = None
        self._build_ui()
        if self.gateway:
            self.gateway.start_ingress()
        self._initialize_synchronization(seat_count=20)
        self.recovery_manager.register_sync_callback(self._apply_recovery_sync_update)
        self._create_retry_manager()
        self._start_updates()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _create_styles(self):
        s = ttk.Style(self)
        s.theme_use("clam")
        s.configure("Panel.TLabelframe", padding=(10, 8), font=("Arial", 10, "bold"))
        s.configure("Control.TButton", padding=6)
        s.configure("Highlight.TLabel", background="#FFE5B4", foreground="#000000")
        s.configure("Server.TFrame", padding=6, relief="ridge", borderwidth=1)

    def _build_ui(self):
        """Build the comprehensive multi-panel dashboard layout."""
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)

        self.scroll_canvas = tk.Canvas(self, borderwidth=0, highlightthickness=0)
        self.scrollbar = ttk.Scrollbar(self, orient="vertical", command=self.scroll_canvas.yview)
        self.scroll_canvas.configure(yscrollcommand=self.scrollbar.set)
        self.scroll_canvas.grid(row=0, column=0, sticky="nsew")
        self.scrollbar.grid(row=0, column=1, sticky="ns")

        self.content_frame = ttk.Frame(self.scroll_canvas)
        self.content_window = self.scroll_canvas.create_window(
            (0, 0),
            window=self.content_frame,
            anchor="nw",
        )
        self.content_frame.bind("<Configure>", self._on_content_configure)
        self.scroll_canvas.bind("<Configure>", self._on_scroll_canvas_configure)
        self.bind_all("<MouseWheel>", self._on_mousewheel)

        # Configure the scrollable content grid.
        for row in range(12):
            self.content_frame.rowconfigure(row, weight=0)
        self.content_frame.rowconfigure(7, weight=1)   # Seat Grid
        self.content_frame.rowconfigure(10, weight=1)  # Event Log
        self.content_frame.columnconfigure(0, weight=1)

        # ROW 0: Server Cluster Panel
        self._build_server_cluster_panel()

        # ROW 1: Heartbeat Monitor Panel
        self._build_heartbeat_panel()

        # ROW 2: Retry Dashboard Panel
        self._build_retry_panel()

        # ROW 3: API Gateway Panel
        self._build_gateway_panel()

        # ROW 4: Cluster Topology Panel
        self._build_topology_panel()

        # ROW 5: Failover Dashboard Panel
        self._build_failover_panel()

        # ROW 6: Decision Details Panel
        self._build_decision_details_panel()

        # ROW 7: Seat Reservation Panel
        self._build_seat_panel()

        # ROW 8: Metrics Dashboard Panel
        self._build_metrics_panel()

        # ROW 9: Synchronization Dashboard Panel
        self._build_sync_panel()

        # ROW 10: Event Log Panel
        self._build_log_panel()

        # Control buttons at bottom
        self._build_controls()
        self._build_failover_panel = self._build_failover_panel

    def _on_content_configure(self, event=None):
        self.scroll_canvas.configure(scrollregion=self.scroll_canvas.bbox("all"))

    def _on_scroll_canvas_configure(self, event):
        self.scroll_canvas.itemconfigure(self.content_window, width=event.width)

    def _on_mousewheel(self, event):
        if hasattr(self, "scroll_canvas"):
            self.scroll_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

    def _build_server_cluster_panel(self):
        panel = ttk.LabelFrame(self.content_frame, text="Server Cluster Panel", style="Panel.TLabelframe")
        panel.grid(row=0, column=0, sticky="ew", padx=8, pady=6)
        panel.columnconfigure(tuple(range(3)), weight=1)

        self.servers = []
        self.server_displays = {}
        for i in range(1, 4):
            sv = ServerStatus(panel, f"Server {i}")
            sv.grid(row=0, column=i-1, sticky="ew", padx=4)
            # attach heartbeat monitor and start sending heartbeats
            if getattr(self, 'hb_monitor', None):
                sv.hb_monitor = self.hb_monitor
                sv.start_heartbeat()
            # attach circuit breaker per server
            try:
                br = Breaker(failure_threshold_consecutive=3,
                             failure_rate_threshold=50.0,
                             window_seconds=60.0,
                             recovery_timeout=5.0,
                             half_open_successes=1,
                             on_state_change=lambda old, new, sid=sv.name: self._on_circuit_state_change(sid, old, new))
                sv.set_breaker(br)
            except Exception:
                sv.breaker = None
            self.servers.append(sv)
            
            # Store display label references for updates
            self.server_displays[sv.name] = {
                'frame': sv,
                'status_label': None,
                'metrics_labels': {},
                'weight_label': None,
                'eff_weight_label': None,
                'health_label': None,
            }

    def _build_gateway_panel(self):
        panel = ttk.LabelFrame(self.content_frame, text="API Gateway & Load Balancer Control", style="Panel.TLabelframe")
        panel.grid(row=3, column=0, sticky="ew", padx=8, pady=6)
        panel.columnconfigure((0, 1, 2, 3), weight=1)

        # Strategy selector
        ttk.Label(panel, text="Load Balancing Algorithm:").grid(row=0, column=0, sticky="w", padx=4)
        self.strategy_var = tk.StringVar(value="Round Robin")
        strategies = ["Round Robin", "Weighted Round Robin", "Resource-Aware", "Consistent Hashing", "Adaptive Feedback"]
        strategy_combo = ttk.Combobox(panel, textvariable=self.strategy_var, values=strategies, state="readonly", width=20)
        strategy_combo.grid(row=0, column=1, sticky="ew", padx=4)
        strategy_combo.bind("<<ComboboxSelected>>", self._on_strategy_changed)

        # Selected server display
        ttk.Label(panel, text="Selected Server:").grid(row=0, column=2, sticky="w", padx=4)
        self.selected_server_var = tk.StringVar(value="-")
        ttk.Label(panel, textvariable=self.selected_server_var, font=("Arial", 10, "bold")).grid(row=0, column=3, sticky="w", padx=4)

        # Request counter
        ttk.Label(panel, text="Gateway Requests:").grid(row=1, column=0, sticky="w", padx=4)
        self.request_counter_var = tk.IntVar(value=0)
        ttk.Label(panel, textvariable=self.request_counter_var).grid(row=1, column=1, sticky="w", padx=4)

        # Routing reason
        ttk.Label(panel, text="Routing Reason:").grid(row=1, column=2, sticky="w", padx=4)
        self.reason_var = tk.StringVar(value="-")
        ttk.Label(panel, textvariable=self.reason_var, wraplength=300).grid(row=1, column=3, sticky="w", padx=4)

        # Create gateway after panel is built
        self.gateway = APIGateway(self, panel, self.servers)

    def _build_topology_panel(self):
        panel = ttk.LabelFrame(self.content_frame, text="Cluster Topology", style="Panel.TLabelframe")
        panel.grid(row=4, column=0, sticky="ew", padx=8, pady=6)
        panel.columnconfigure(0, weight=1)

        self.topology_canvas = tk.Canvas(
            panel,
            height=250,
            bg="#FAFAFA",
            highlightthickness=0,
        )
        self.topology_canvas.grid(row=0, column=0, sticky="ew")
        self.topology_canvas.bind("<Configure>", lambda event: self._draw_topology())
        self._draw_topology()

    def _draw_topology(self):
        if not hasattr(self, "topology_canvas"):
            return

        canvas = self.topology_canvas
        canvas.delete("all")
        self._topology_nodes.clear()
        self._topology_edges.clear()
        self._topology_labels.clear()

        width = max(canvas.winfo_width(), 900)
        center_x = width // 2
        node_w = 180
        node_h = 34
        server_w = 150
        server_h = 34

        client = (center_x, 28)
        gateway = (center_x, 86)
        load_balancer = (center_x, 144)
        server_y = 212
        server_count = max(1, len(getattr(self, "servers", []) or []))
        spacing = min(240, max(160, width // (server_count + 1)))
        start_x = center_x - ((server_count - 1) * spacing // 2)

        self._draw_topology_node("Client", "Client", client[0], client[1], node_w, node_h)
        self._draw_topology_node("Gateway", "Gateway", gateway[0], gateway[1], node_w, node_h)
        self._draw_topology_node(
            "Load Balancer",
            f"Load Balancer\n{self.gateway._current_strategy_name if self.gateway else self.strategy_var.get()}",
            load_balancer[0],
            load_balancer[1],
            node_w,
            node_h + 12,
        )

        self._topology_edges["Client->Gateway"] = self._draw_topology_arrow(
            client[0],
            client[1] + node_h // 2,
            gateway[0],
            gateway[1] - node_h // 2,
        )
        self._topology_edges["Gateway->Load Balancer"] = self._draw_topology_arrow(
            gateway[0],
            gateway[1] + node_h // 2,
            load_balancer[0],
            load_balancer[1] - (node_h + 12) // 2,
        )

        for idx, srv in enumerate(getattr(self, "servers", [])):
            x = start_x + (idx * spacing)
            state = "offline" if srv.status != "online" else "idle"
            self._draw_topology_node(srv.name, srv.name, x, server_y, server_w, server_h, state=state)
            self._topology_edges[f"Load Balancer->{srv.name}"] = self._draw_topology_arrow(
                load_balancer[0],
                load_balancer[1] + (node_h + 12) // 2,
                x,
                server_y - server_h // 2,
            )

        self._highlight_routing_path()

    def _draw_topology_node(self, key, text, x, y, width, height, state="idle"):
        fill = "#FFFFFF"
        outline = "#90A4AE"
        if state == "offline":
            fill = "#FFEBEE"
            outline = "#E53935"

        rect = self.topology_canvas.create_rectangle(
            x - width // 2,
            y - height // 2,
            x + width // 2,
            y + height // 2,
            fill=fill,
            outline=outline,
            width=2,
        )
        label = self.topology_canvas.create_text(
            x,
            y,
            text=text,
            fill="#263238",
            font=("Arial", 10, "bold"),
            justify="center",
        )
        self._topology_nodes[key] = rect
        self._topology_labels[key] = label

    def _draw_topology_arrow(self, x1, y1, x2, y2):
        return self.topology_canvas.create_line(
            x1,
            y1,
            x2,
            y2,
            arrow=tk.LAST,
            fill="#B0BEC5",
            width=2,
            arrowshape=(10, 12, 4),
        )

    def set_active_route(self, server_id):
        self._active_route_server = server_id
        self._highlight_routing_path()

    def _highlight_routing_path(self):
        if not hasattr(self, "topology_canvas"):
            return

        active_color = "#1976D2"
        idle_color = "#B0BEC5"
        offline_fill = "#FFEBEE"
        offline_outline = "#E53935"
        idle_fill = "#FFFFFF"
        idle_outline = "#90A4AE"
        active_fill = "#E3F2FD"

        for edge_id in self._topology_edges.values():
            self.topology_canvas.itemconfig(edge_id, fill=idle_color, width=2)

        for key, node_id in self._topology_nodes.items():
            srv = next((server for server in getattr(self, "servers", []) if server.name == key), None)
            if srv and srv.status != "online":
                self.topology_canvas.itemconfig(node_id, fill=offline_fill, outline=offline_outline, width=2)
            else:
                self.topology_canvas.itemconfig(node_id, fill=idle_fill, outline=idle_outline, width=2)

        if self.gateway and "Load Balancer" in self._topology_labels:
            self.topology_canvas.itemconfig(
                self._topology_labels["Load Balancer"],
                text=f"Load Balancer\n{self.gateway._current_strategy_name}",
            )

        active_server = self._active_route_server
        server = next((srv for srv in getattr(self, "servers", []) if srv.name == active_server), None)
        if not active_server or not server or server.status != "online":
            return

        active_edges = [
            "Client->Gateway",
            "Gateway->Load Balancer",
            f"Load Balancer->{active_server}",
        ]
        active_nodes = ["Client", "Gateway", "Load Balancer", active_server]

        for edge_key in active_edges:
            edge_id = self._topology_edges.get(edge_key)
            if edge_id:
                self.topology_canvas.itemconfig(edge_id, fill=active_color, width=4)

        for node_key in active_nodes:
            node_id = self._topology_nodes.get(node_key)
            if node_id:
                self.topology_canvas.itemconfig(node_id, fill=active_fill, outline=active_color, width=3)

    def _build_decision_details_panel(self):
        panel = ttk.LabelFrame(self.content_frame, text="Load Balancer Decision Details", style="Panel.TLabelframe")
        panel.grid(row=6, column=0, sticky="ew", padx=8, pady=6)
        panel.columnconfigure(tuple(range(6)), weight=1)

        self.decision_text = tk.Text(panel, height=5, width=120, state="disabled", wrap="word")
        self.decision_text.grid(row=0, column=0, columnspan=6, sticky="nsew", padx=4, pady=4)

    def update_decision_details(self, algorithm, selected_server, routing_key, routing_reason, score_calculation):
        """Populate the decision panel with the latest load-balancer decision."""
        self.last_decision_details = {
            "algorithm": algorithm,
            "selected_server": selected_server,
            "routing_key": routing_key,
            "routing_reason": routing_reason,
            "score_calculation": score_calculation,
        }
        details = (
            f"Algorithm: {algorithm}\n"
            f"Selected Server: {selected_server}\n"
            f"Routing Key: {routing_key}\n"
            f"Routing Reason: {routing_reason}\n"
            f"Score Calculation: {score_calculation}"
        )
        try:
            self.decision_text.configure(state="normal")
            self.decision_text.delete("1.0", "end")
            self.decision_text.insert("end", details)
            self.decision_text.configure(state="disabled")
        except Exception:
            pass

    def _build_seat_panel(self):
        panel = ttk.LabelFrame(self.content_frame, text="Seat Reservation Panel", style="Panel.TLabelframe")
        panel.grid(row=7, column=0, sticky="nsew", padx=8, pady=6)
        panel.columnconfigure(0, weight=1)
        panel.rowconfigure(0, weight=1)

        inner = ttk.Frame(panel)
        inner.grid(row=0, column=0, sticky="nsew")
        inner.columnconfigure(tuple(range(5)), weight=1)

        self.seat_buttons = {}
        seat_count = 20
        cols = 5
        for i in range(seat_count):
            r = i // cols
            c = i % cols
            seat_id = i + 1
            btn = SeatButton(
                inner,
                seat_id,
                gateway=self.gateway,
                on_change=self._on_seat_change,
                lease_manager=self.lease_manager,
            )
            btn.grid(row=r, column=c, padx=6, pady=6, sticky="nsew")
            self.seat_buttons[seat_id] = btn

    def _build_metrics_panel(self):
        panel = ttk.LabelFrame(self.content_frame, text="Real-Time Observability Dashboard", style="Panel.TLabelframe")
        panel.grid(row=8, column=0, sticky="ew", padx=8, pady=6)
        panel.columnconfigure(tuple(range(6)), weight=1)

        # Create metric variables first
        self.rps_var = tk.StringVar(value="0.00")
        self.success_rate_var = tk.StringVar(value="100.0%")
        self.failure_rate_var = tk.StringVar(value="0.0%")
        self.current_lb_var = tk.StringVar(value="Round Robin")
        self.current_circuit_state_var = tk.StringVar(value="CLOSED")
        self.active_servers_var = tk.StringVar(value="-")

        metrics = [
            ("Requests Per Second", self.rps_var),
            ("Success Rate", self.success_rate_var),
            ("Failure Rate", self.failure_rate_var),
            ("Current Load Balancer", self.current_lb_var),
            ("Current Circuit Breaker State", self.current_circuit_state_var),
            ("Active Servers", self.active_servers_var),
        ]

        for col, (label, var) in enumerate(metrics):
            cell = ttk.Frame(panel, padding=(8, 4))
            cell.grid(row=0, column=col, sticky="nsew", padx=4)
            ttk.Label(cell, text=label).grid(row=0, column=0, sticky="w")
            ttk.Label(cell, textvariable=var, font=("Arial", 11, "bold")).grid(row=1, column=0, sticky="w")

    def _build_heartbeat_panel(self):
        panel = ttk.LabelFrame(self.content_frame, text="Heartbeat Monitor", style="Panel.TLabelframe")
        panel.grid(row=1, column=0, sticky="ew", padx=8, pady=6)
        # Dynamic columns for each server
        cols = len(getattr(self, 'servers', []) or [])
        if cols == 0:
            cols = 3
        panel.columnconfigure(tuple(range(cols)), weight=1)

        # Vars per server
        self.hb_last_vars = {}
        self.hb_latency_vars = {}
        self.hb_health_vars = {}

        for idx in range(cols):
            name = f"Server {idx+1}"
            last = tk.StringVar(value='-')
            lat = tk.IntVar(value=0)
            health = tk.StringVar(value='Unknown')
            self.hb_last_vars[name] = last
            self.hb_latency_vars[name] = lat
            self.hb_health_vars[name] = health

            ttk.Label(panel, text=name+":").grid(row=0, column=idx, sticky='w')
            ttk.Label(panel, textvariable=last).grid(row=1, column=idx, sticky='w')
            ttk.Label(panel, textvariable=lat).grid(row=2, column=idx, sticky='w')
            ttk.Label(panel, textvariable=health).grid(row=3, column=idx, sticky='w')

        # Alerts area
        self.hb_alerts = tk.Text(panel, height=3, state='disabled', wrap='word')
        self.hb_alerts.grid(row=4, column=0, columnspan=cols, sticky='nsew', pady=(6,0))

    def _build_retry_panel(self):
        panel = ttk.LabelFrame(self.content_frame, text="Retry Dashboard", style="Panel.TLabelframe")
        panel.grid(row=2, column=0, sticky="ew", padx=8, pady=6)
        panel.columnconfigure((0, 1, 2, 3), weight=1)

        self.retry_count_var = tk.IntVar(value=0)
        self.retry_delay_var = tk.StringVar(value='-')
        self.backoff_stage_var = tk.IntVar(value=0)
        self.retry_status_var = tk.StringVar(value="Idle")

        ttk.Label(panel, text="Retry Count:").grid(row=0, column=0, sticky="w", padx=4)
        ttk.Label(panel, textvariable=self.retry_count_var).grid(row=0, column=1, sticky="w", padx=4)

        ttk.Label(panel, text="Next Retry Delay:").grid(row=0, column=2, sticky="w", padx=4)
        ttk.Label(panel, textvariable=self.retry_delay_var).grid(row=0, column=3, sticky="w", padx=4)

        ttk.Label(panel, text="Backoff Stage:").grid(row=1, column=0, sticky="w", padx=4)
        ttk.Label(panel, textvariable=self.backoff_stage_var).grid(row=1, column=1, sticky="w", padx=4)

        ttk.Label(panel, text="Retry Status:").grid(row=1, column=2, sticky="w", padx=4)
        ttk.Label(panel, textvariable=self.retry_status_var).grid(row=1, column=3, sticky="w", padx=4)

        ttk.Button(panel, text="Simulate Retry", command=self._start_retry_simulation).grid(row=2, column=0, columnspan=4, pady=(8,0))

    def _build_failover_panel(self):
        panel = ttk.LabelFrame(self.content_frame, text="Failover Dashboard", style="Panel.TLabelframe")
        panel.grid(row=5, column=0, sticky="ew", padx=8, pady=6)
        panel.columnconfigure((0, 1), weight=1)

        ttk.Label(panel, text="Current Primary Server:").grid(row=0, column=0, sticky="w", padx=4)
        ttk.Label(panel, textvariable=self.primary_server_var, font=("Arial", 10, "bold")).grid(row=0, column=1, sticky="w", padx=4)

        ttk.Label(panel, text="Active Failovers:").grid(row=1, column=0, sticky="w", padx=4)
        ttk.Label(panel, textvariable=self.active_failovers_var, wraplength=800).grid(row=1, column=1, sticky="w", padx=4)

        ttk.Label(panel, text="Recovery Events:").grid(row=2, column=0, sticky="nw", padx=4, pady=(4,0))
        self.failover_events_text = tk.Text(panel, height=4, width=120, state="disabled", wrap="word")
        self.failover_events_text.grid(row=2, column=1, sticky="ew", padx=4, pady=(4,0))

    def _update_failover_panel(self):
        active_list = sorted(self._active_failovers)
        if active_list:
            self.active_failovers_var.set(", ".join(active_list))
        else:
            self.active_failovers_var.set("None")

        self.primary_server_var.set(self.selected_server_var.get() or "-")

        # Keep only the last 5 recovery events in the panel
        latest_events = self.recovery_event_history[-5:]
        try:
            self.failover_events_text.configure(state="normal")
            self.failover_events_text.delete("1.0", "end")
            for event in latest_events:
                self.failover_events_text.insert("end", event + "\n")
            self.failover_events_text.configure(state="disabled")
        except Exception:
            pass

        self.after(1000, self._update_failover_panel)

    def _create_retry_manager(self):
        self.retry_manager = RetryManager(
            max_attempts=5,
            base_delay=1.0,
            backoff_factor=2.0,
            max_delay=16.0,
            jitter=True,
            on_retry=self._on_retry_event,
            on_giveup=self._on_retry_giveup,
            logger=self._retry_logger,
        )
        self.booking_retry_manager = RetryManager(
            max_attempts=3,
            base_delay=0.25,
            backoff_factor=2.0,
            max_delay=1.0,
            jitter=True,
            retry_exceptions=(TransientBookingError,),
            on_retry=self._on_retry_event,
            on_giveup=self._on_retry_giveup,
            logger=self._retry_logger,
        )
        self._retry_simulation_count = 0

    def _on_retry_event(self, attempt: int, exc: BaseException, delay: float) -> None:
        def ui_update():
            self.retry_count_var.set(attempt)
            self.retry_delay_var.set(f"{delay:.2f}s")
            self.backoff_stage_var.set(attempt)
            self.retry_status_var.set("Retrying")
            self.append_log(f"Retry attempt {attempt}: {exc} -> next in {delay:.2f}s")
        self.after(0, ui_update)

    def _on_retry_giveup(self, exc: BaseException) -> None:
        def ui_update():
            self.retry_status_var.set("Failed")
            self.append_log(f"Retry giving up: {exc}")
        self.after(0, ui_update)

    def _retry_logger(self, message: str) -> None:
        self.after(0, lambda: self.append_log(message))

    def _start_retry_simulation(self):
        self.retry_status_var.set("Starting")
        self.retry_count_var.set(0)
        self.retry_delay_var.set("-")
        self.backoff_stage_var.set(0)
        thread = threading.Thread(target=self._run_retry_simulation, daemon=True)
        thread.start()

    def _run_retry_simulation(self):
        try:
            self.retry_manager.call(self._retry_prone_operation)
            self.after(0, lambda: self.retry_status_var.set("Succeeded"))
        except Exception as exc:
            self.after(0, lambda: self.retry_status_var.set("Failed"))
            self.after(0, lambda: self.append_log(f"Retry simulation ended: {exc}"))

    def _retry_prone_operation(self):
        self._retry_simulation_count += 1
        if self._retry_simulation_count < 4:
            raise RuntimeError(f"Simulated transient failure #{self._retry_simulation_count}")
        return "ok"

    def _update_heartbeat_panel(self):
        # Update heartbeat metrics from monitor
        try:
            for srv in getattr(self, 'servers', []):
                name = srv.name
                status = self.hb_monitor.get_server_status(name)
                last = status.get('last_heartbeat') or '-'
                latency = status.get('time_since_heartbeat_ms') or 0
                is_alive = status.get('is_alive', False)
                self.hb_last_vars.get(name, tk.StringVar()).set(last)
                self.hb_latency_vars.get(name, tk.IntVar()).set(latency)
                self.hb_health_vars.get(name, tk.StringVar()).set('UP' if is_alive else 'DOWN')
        except Exception:
            pass
        # schedule next update
        self.after(1000, self._update_heartbeat_panel)

    def _build_sync_panel(self):
        panel = ttk.LabelFrame(self.content_frame, text="Synchronization Dashboard", style="Panel.TLabelframe")
        panel.grid(row=9, column=0, sticky="ew", padx=8, pady=6)
        panel.columnconfigure(tuple(range(8)), weight=1)

        # Create sync tracking variables
        self.last_sync_event_var = tk.StringVar(value="-")
        self.sync_delay_var = tk.IntVar(value=0)
        self.pending_repl_var = tk.IntVar(value=0)
        self.repl_status_var = tk.StringVar(value="Healthy")
        self.server1_version_var = tk.IntVar(value=0)
        self.server2_version_var = tk.IntVar(value=0)
        self.server3_version_var = tk.IntVar(value=0)
        self.consistency_lag_var = tk.IntVar(value=0)

        # Display sync metrics
        sync_metrics = [
            ("Last Sync Event", self.last_sync_event_var),
            ("Sync Delay (ms)", self.sync_delay_var),
            ("Pending Repl", self.pending_repl_var),
            ("Repl Status", self.repl_status_var),
            ("S1 Version", self.server1_version_var),
            ("S2 Version", self.server2_version_var),
            ("S3 Version", self.server3_version_var),
            ("Max Lag (ms)", self.consistency_lag_var),
        ]

        for col, (label, var) in enumerate(sync_metrics):
            ttk.Label(panel, text=f"{label}:").grid(row=0, column=col, sticky="w", padx=4)
            ttk.Label(panel, textvariable=var, font=("Courier", 9)).grid(row=1, column=col, sticky="w", padx=4)

    def _initialize_synchronization(self, seat_count: int) -> None:
        """Create one seat replica store per simulated booking server."""
        server_ids = [srv.name for srv in getattr(self, "servers", [])]
        self.state_stores = {}
        self.sync_replicators = {}

        for server_id in server_ids:
            store = StateStore(server_id)
            store.initialize_seats(seat_count)
            self.state_stores[server_id] = store

        for server_id, store in self.state_stores.items():
            self.sync_replicators[server_id] = StateReplicator(
                store,
                peers=server_ids,
            )

        primary_replicator = self.sync_replicators.get("Server 1")
        if primary_replicator:
            self.consistency_simulator = EventualConsistencySimulator(
                primary_replicator,
                min_delay_ms=100,
                max_delay_ms=600,
            )
            self.consistency_simulator.set_sync_callback(self._on_sync_delivered)

        self._refresh_sync_status(repl_status="Healthy")

    def sync_seat_state(self, seat_id: int, state: str, owner: str = "") -> None:
        """Update Server 1 then replicate the seat state to the other stores."""
        primary_store = self.state_stores.get("Server 1")
        if not primary_store or not self.consistency_simulator:
            return

        seat = primary_store.update_seat(seat_id, state, owner=owner or "")
        if not seat:
            return

        self.append_log(
            f"[SYNC] Server 1 updated Seat {seat_id} -> {state} "
            f"(owner={owner or '-'}, version={seat.version})"
        )
        self.consistency_simulator.broadcast_with_delay(seat)
        self._refresh_sync_status(repl_status="Replicating")

    def _on_sync_delivered(self, peer_id, seat, delay_ms):
        """Marshal timer-thread sync delivery back onto the Tk event loop."""
        self.after(0, lambda: self._apply_sync_delivery(peer_id, seat, delay_ms))

    def _apply_sync_delivery(self, peer_id, seat, delay_ms):
        peer_replicator = self.sync_replicators.get(peer_id)
        if peer_replicator:
            peer_replicator.apply_remote_update(seat, source_server="Server 1")

        self.log_sync_event(seat.seat_id, peer_id, delay_ms)
        self._refresh_sync_status(repl_status="Healthy")

    def _apply_recovery_sync_update(self, server_id, seat):
        peer_replicator = self.sync_replicators.get(server_id)
        if peer_replicator:
            peer_replicator.apply_remote_update(seat, source_server="Server 1")
            self.append_log(
                f"[RECOVERY] {server_id} caught up Seat {seat.seat_id} "
                f"(state={seat.state}, version={seat.version})"
            )
            self._refresh_sync_status(repl_status="Healthy")

    def _store_version(self, server_id: str) -> int:
        store = self.state_stores.get(server_id)
        if not store:
            return 0
        seats = store.list_seats()
        return max((seat.version for seat in seats), default=0)

    def _refresh_sync_status(self, repl_status: str = "Healthy") -> None:
        pending_count = 0
        max_lag_ms = 0
        if self.consistency_simulator:
            pending_count = self.consistency_simulator.get_pending_count()
            if pending_count:
                lags = self.consistency_simulator.get_consistency_lag_ms()
                max_lag_ms = max(lags.values(), default=0)

        self.update_sync_status(
            repl_status=repl_status if pending_count else "Healthy",
            pending_count=pending_count,
            max_lag_ms=max_lag_ms,
            s1_ver=self._store_version("Server 1"),
            s2_ver=self._store_version("Server 2"),
            s3_ver=self._store_version("Server 3"),
        )

    def _build_log_panel(self):
        panel = ttk.LabelFrame(self.content_frame, text="Event Log Panel", style="Panel.TLabelframe")
        panel.grid(row=10, column=0, sticky="nsew", padx=8, pady=6)
        panel.columnconfigure(0, weight=1)
        panel.rowconfigure(0, weight=1)

        self.log_text = tk.Text(panel, width=150, height=10, state="disabled", wrap="word")
        self.log_text.grid(row=0, column=0, sticky="nsew")
        
        log_scroll = ttk.Scrollbar(panel, command=self.log_text.yview)
        log_scroll.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=log_scroll.set)

    def _build_controls(self):
        controls = ttk.Frame(self.content_frame)
        controls.grid(row=11, column=0, sticky="ew", padx=8, pady=8)
        
        ttk.Button(controls, text="Reset Seats", command=self.reset_seats).pack(side="left", padx=4)
        ttk.Button(controls, text="Clear Log", command=self.clear_log).pack(side="left", padx=4)

    def _on_close(self):
        try:
            if self.gateway:
                self.gateway.stop_ingress()
        except Exception:
            pass
        try:
            self.failure_detector.stop(timeout=1.0)
        except Exception:
            pass
        self.destroy()

    def _on_strategy_changed(self, event=None):
        if self.gateway:
            try:
                self.gateway.strategy_var.set(self.strategy_var.get())
            except Exception:
                pass
            self.gateway._on_strategy_changed(event)

    def _start_updates(self):
        """Schedule periodic updates of server metrics display."""
        # Register failure detector callbacks and start background detector
        try:
            self.failure_detector.register_failure_callback(self._on_server_failure)
            self.failure_detector.register_recovery_callback(self._on_server_recovery)
            self.failure_detector.start()
        except Exception:
            pass

        # Kick off periodic updates
        self._update_server_displays()
        self._update_heartbeat_panel()
        self._update_failover_panel()
        self._update_observability_dashboard()

        # Ensure servers send heartbeats (in case they were created earlier)
        for srv in getattr(self, 'servers', []):
            if getattr(srv, 'hb_monitor', None):
                try:
                    srv.start_heartbeat()
                except Exception:
                    pass

    def record_request(self) -> None:
        """Record an incoming gateway request for observability metrics."""
        now = time.time()
        self.request_counter += 1
        self._request_events.append(now)
        try:
            self.request_counter_var.set(self.request_counter)
        except Exception:
            pass

    def record_success(self) -> None:
        """Record a completed successful request."""
        self.success_count += 1

    def record_failure(self) -> None:
        """Record a completed failed request."""
        self.failure_count += 1

    def _update_observability_dashboard(self):
        """Refresh real-time observability metrics."""
        now = time.time()
        one_second_ago = now - 1.0
        self._request_events = [ts for ts in self._request_events if ts >= one_second_ago]

        completed = self.success_count + self.failure_count
        if completed:
            success_rate = (self.success_count / completed) * 100.0
            failure_rate = (self.failure_count / completed) * 100.0
        else:
            success_rate = 100.0
            failure_rate = 0.0

        current_lb = self.gateway._current_strategy_name if self.gateway else self.strategy_var.get()
        active_servers = self._get_active_servers()

        self.rps_var.set(f"{len(self._request_events):.2f}")
        self.success_rate_var.set(f"{success_rate:.1f}%")
        self.failure_rate_var.set(f"{failure_rate:.1f}%")
        self.current_lb_var.set(current_lb)
        self.current_circuit_state_var.set(self._get_current_circuit_state())
        self.active_servers_var.set(", ".join(active_servers) if active_servers else "None")

        self.after(1000, self._update_observability_dashboard)

    def _get_current_circuit_state(self) -> str:
        states = []
        for srv in getattr(self, "servers", []):
            breaker = getattr(srv, "breaker", None)
            if not breaker:
                continue
            try:
                states.append(breaker.get_state())
            except Exception:
                pass

        if not states:
            return "UNKNOWN"
        if any(state == CircuitState.OPEN for state in states):
            return "OPEN"
        if any(state == CircuitState.HALF_OPEN for state in states):
            return "HALF_OPEN"
        return "CLOSED"

    def _get_active_servers(self):
        active = []
        for srv in getattr(self, "servers", []):
            if srv.status != "online":
                continue

            breaker = getattr(srv, "breaker", None)
            try:
                if breaker and breaker.get_state() == CircuitState.OPEN:
                    continue
            except Exception:
                pass
            active.append(srv.name)
        return active

    def _on_circuit_state_change(self, server_id, old_state, new_state):
        def ui_update():
            self.telemetry_collector.record_event(
                "circuit_state",
                f"{server_id} circuit {old_state.value} -> {new_state.value}",
                server_id=server_id,
                circuit_state=new_state.value,
            )
            self.append_log(f"[CIRCUIT] {server_id}: {old_state.value} -> {new_state.value}")
        try:
            self.after(0, ui_update)
        except Exception:
            pass

    def _on_server_failure(self, server_id: str) -> None:
        # Append failure alert to alerts area and event log
        msg = f"[FAILURE DETECTOR] Server {server_id} heartbeat timeout"
        try:
            self.hb_alerts.configure(state='normal')
            self.hb_alerts.insert('end', msg + "\n")
            self.hb_alerts.see('end')
            self.hb_alerts.configure(state='disabled')
        except Exception:
            pass
        self.append_log(msg)
        self.failover_manager.update_health(server_id, healthy=False, timestamp=time.time(), reason="heartbeat timeout")
        self._pending_failover_servers.add(server_id)
        self._active_failovers.add(server_id)

    def _on_server_recovery(self, server_id: str) -> None:
        msg = f"[FAILURE DETECTOR] Server {server_id} recovered"
        try:
            self.hb_alerts.configure(state='normal')
            self.hb_alerts.insert('end', msg + "\n")
            self.hb_alerts.see('end')
            self.hb_alerts.configure(state='disabled')
        except Exception:
            pass
        self.append_log(msg)
        self.failover_manager.update_health(server_id, healthy=True, timestamp=time.time(), reason="recovered")
        self.recovery_manager.mark_recovered(server_id, reason="heartbeat recovered")
        peer_ids = [srv.name for srv in getattr(self, "servers", []) if srv.name != server_id]
        self.recovery_manager.rejoin_cluster(server_id, peer_ids=peer_ids)
        primary_store = self.state_stores.get("Server 1")
        if primary_store and server_id != "Server 1":
            self.recovery_manager.receive_sync_updates(server_id, primary_store.list_seats())
        if server_id in self._pending_failover_servers:
            self._pending_failover_servers.discard(server_id)
        if server_id in self._active_failovers:
            self._active_failovers.discard(server_id)
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.recovery_event_history.append(f"[{timestamp}] Server {server_id} rejoined cluster")

    def _update_server_displays(self):
        """Update server display with real-time values."""
        if self.gateway:
            strategy_name = self.gateway._current_strategy_name
            
            for srv in self.servers:
                # Calculate health score for Adaptive Feedback
                health_score = 100 - srv.cpu - (srv.connections * 2) - (srv.requests * 0.1)
                
                # For Weighted RR, calculate effective weight
                eff_weight = 1.0
                if strategy_name == "Weighted Round Robin":
                    weights_map = {"Server 1": 5, "Server 2": 3, "Server 3": 1}
                    base_weight = weights_map.get(srv.name, 1)
                    # Effective weight decreases with CPU usage
                    eff_weight = base_weight * (1.0 - srv.cpu / 100.0)
                
                # Update display info (for future UI updates)
                display_info = self.server_displays.get(srv.name, {})
                display_info['health_score'] = max(0, health_score)
                display_info['eff_weight'] = max(0, eff_weight)
                # Update circuit breaker UI and visual state if breaker present
                try:
                    br = getattr(srv, 'breaker', None)
                    if br:
                        st = br.get_state().value
                        srv.breaker_state_var.set(st)
                        s_count, f_count = br.get_counts()
                        srv.breaker_fail_var.set(f_count)
                        srv.breaker_rate_var.set(round(br.failure_percentage(), 1))
                        # cooldown remaining
                        try:
                            cb = br.cb
                            last = getattr(cb, '_last_failure_time', 0)
                            timeout = getattr(cb, '_recovery_timeout', 0)
                            rem = int(max(0, timeout - (time.time() - last))) if st == CircuitState.OPEN.value else 0
                        except Exception:
                            rem = 0
                        srv.breaker_cd_var.set(f"{rem}s" if rem > 0 else ('-' if st != CircuitState.OPEN.value else '0s'))
                        # color mapping for visual transitions
                        if srv.status == 'offline':
                            color = '#F44336'
                        else:
                            if st == CircuitState.CLOSED.value:
                                color = '#4CAF50'
                            elif st == CircuitState.HALF_OPEN.value:
                                color = '#FFEB3B'
                            elif st == CircuitState.OPEN.value:
                                color = '#9C27B0'
                            else:
                                color = '#BDBDBD'
                        try:
                            srv.canvas.itemconfig(srv.oval, fill=color)
                        except Exception:
                            pass
                except Exception:
                    pass

            self._highlight_routing_path()
        
        # Schedule next update
        self.after(1000, self._update_server_displays)


    def _on_seat_change(self, seat_id, state):
        # Update seat metrics (optional - for internal tracking)
        reserved = sum(1 for b in self.seat_buttons.values() if b.state == "reserved")
        locked = sum(1 for b in self.seat_buttons.values() if b.state == "locked")
        total = len(self.seat_buttons)
        available = total - reserved - locked
        
        # Append to log with event type
        if seat_id != 0:
            event_msg = f"Seat {seat_id} -> {state}"
            if state == "locked":
                event_msg += " (lock acquired)"
            elif state == "reserved":
                event_msg += " (booking confirmed)"
            self.append_log(event_msg)

            if state in ("locked", "reserved"):
                seat_btn = self.seat_buttons.get(seat_id)
                owner = getattr(seat_btn, "owner", "") if seat_btn else ""
                self.sync_seat_state(seat_id, state, owner=owner)

    def append_log(self, text):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {text}"
        self.log_text.configure(state="normal")
        self.log_text.insert("end", log_entry + "\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def clear_log(self):
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.configure(state="disabled")

    def reset_seats(self):
        for b in self.seat_buttons.values():
            # cancel any locks and reset to available
            b.cancel_lock()
        if self.consistency_simulator:
            for seat_id in self.seat_buttons:
                self.consistency_simulator.cancel_pending_replication(seat_id)
        self._initialize_synchronization(seat_count=len(self.seat_buttons))
        self._on_seat_change(0, "reset")

    def log_sync_event(self, seat_id: int, peer_server: str, delay_ms: int) -> None:
        """Log a synchronization event to both sync panel and event log."""
        # Update sync panel variables
        self.last_sync_event_var.set(f"Seat {seat_id}")
        self.sync_delay_var.set(delay_ms)
        
        # Add to event log with SYNC tag
        self.append_log(f"[SYNC] Seat {seat_id} replicated to {peer_server} ({delay_ms}ms)")

    def update_sync_status(self, repl_status: str = "Healthy", pending_count: int = 0, 
                          max_lag_ms: int = 0, s1_ver: int = 0, s2_ver: int = 0, 
                          s3_ver: int = 0) -> None:
        """Update synchronization dashboard metrics."""
        self.repl_status_var.set(repl_status)
        self.pending_repl_var.set(pending_count)
        self.consistency_lag_var.set(max_lag_ms)
        self.server1_version_var.set(s1_ver)
        self.server2_version_var.set(s2_ver)
        self.server3_version_var.set(s3_ver)


if __name__ == "__main__":
    app = Dashboard()
    app.mainloop()
