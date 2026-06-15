from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
import random
import threading
import time
from typing import Dict, List

from flask import Flask, jsonify, render_template_string, request

from circuit_breaker import Breaker, CircuitState
from circuit_breaker.state_machine import CircuitOpenError
from failover.failover_manager import FailoverManager
from failover.recovery_manager import RecoveryManager
from load_balancer import (
    AdaptiveFeedbackStrategy,
    ConsistentHashingStrategy,
    LoadBalancer,
    ResourceAwareLeastConnectionsStrategy,
    RoundRobinStrategy,
    ServerMetrics,
    WeightedRoundRobinStrategy,
)
from locking import DistributedLeaseManager, LeaseConflictError, LeaseNotFoundError
from monitoring import HeartbeatMonitor
from retry.retry_manager import RetryError, RetryManager
from synchronization import EventualConsistencySimulator, StateReplicator, StateStore
from telemetry.metrics_collector import MetricsCollector


SERVER_IDS = ("Server 1", "Server 2", "Server 3")
SEAT_COUNT = 20
STRATEGIES = (
    "Round Robin",
    "Weighted Round Robin",
    "Resource-Aware",
    "Consistent Hashing",
    "Adaptive Feedback",
)


class TransientBookingError(Exception):
    """Retryable booking failure."""


class NonRetryableBookingError(Exception):
    """Booking failure that should not be retried."""


class DistributedTicketWebApp:
    """Web-backed simulator state.

    The old Tkinter widgets were removed from `dashboard.py`; the distributed
    system modules are still used directly by this web state layer.
    """

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.logs: List[str] = []
        self.request_timestamps: List[float] = []
        self.success_count = 0
        self.failure_count = 0
        self.strategy_name = "Round Robin"
        self.last_route: Dict[str, object] = {}
        self.retry_status = {
            "count": 0,
            "next_delay": 0.0,
            "stage": "Idle",
            "status": "Ready",
        }
        self.servers = {
            server_id: {
                "online": True,
                "cpu": random.randint(15, 55),
                "connections": random.randint(0, 6),
                "response_time": random.randint(45, 180),
                "requests": 0,
            }
            for server_id in SERVER_IDS
        }
        self.seats = {
            seat_id: {
                "state": "available",
                "owner": "",
                "lease_id": "",
                "expires_at": 0.0,
                "server": "",
            }
            for seat_id in range(1, SEAT_COUNT + 1)
        }

        self.lease_manager = DistributedLeaseManager(default_ttl_seconds=30)
        self.heartbeat_monitor = HeartbeatMonitor(heartbeat_interval_sec=5, timeout_sec=15)
        self.failover_manager = FailoverManager(logger=self.log)
        self.recovery_manager = RecoveryManager(logger=self.log)
        self.metrics_collector = MetricsCollector(logger=lambda _message: None)
        self.breakers = {
            server_id: Breaker(
                failure_threshold_consecutive=3,
                failure_rate_threshold=60,
                window_seconds=60,
                recovery_timeout=8,
                half_open_successes=1,
                on_state_change=self._breaker_callback(server_id),
            )
            for server_id in SERVER_IDS
        }
        self.load_balancer = LoadBalancer(self._strategy_for(self.strategy_name))
        self.state_stores: Dict[str, StateStore] = {}
        self.replicators: Dict[str, StateReplicator] = {}
        self.consistency_simulator = None
        self._initialize_synchronization()
        self.recovery_manager.register_sync_callback(self._apply_recovery_sync_update)
        self._record_initial_heartbeats()
        self.log("[SYSTEM] Flask web dashboard initialized")

    def _breaker_callback(self, server_id: str):
        def callback(old_state: CircuitState, new_state: CircuitState) -> None:
            self.metrics_collector.record_event(
                "circuit_state",
                f"{server_id}: {old_state.value} -> {new_state.value}",
                server_id=server_id,
                circuit_state=new_state.value,
            )
            self.log(f"[CIRCUIT] {server_id}: {old_state.value} -> {new_state.value}")

        return callback

    def _record_initial_heartbeats(self) -> None:
        for server_id in SERVER_IDS:
            self.heartbeat_monitor.record_heartbeat(server_id)
            self.failover_manager.update_health(server_id, True, time.time(), "initial heartbeat")

    def _initialize_synchronization(self) -> None:
        self.state_stores = {}
        self.replicators = {}
        for server_id in SERVER_IDS:
            store = StateStore(server_id)
            store.initialize_seats(SEAT_COUNT)
            self.state_stores[server_id] = store
        for server_id, store in self.state_stores.items():
            self.replicators[server_id] = StateReplicator(store, list(SERVER_IDS))

        primary = self.replicators["Server 1"]
        self.consistency_simulator = EventualConsistencySimulator(
            primary,
            min_delay_ms=150,
            max_delay_ms=900,
        )
        self.consistency_simulator.set_sync_callback(self._on_sync_delivered)

    def _strategy_for(self, name: str):
        strategies = {
            "Round Robin": RoundRobinStrategy(),
            "Weighted Round Robin": WeightedRoundRobinStrategy(
                weights={"Server 1": 3, "Server 2": 2, "Server 3": 1}
            ),
            "Resource-Aware": ResourceAwareLeastConnectionsStrategy(),
            "Consistent Hashing": ConsistentHashingStrategy(vnode_count=50),
            "Adaptive Feedback": AdaptiveFeedbackStrategy(),
        }
        return strategies[name]

    def set_strategy(self, name: str) -> None:
        if name not in STRATEGIES:
            name = "Round Robin"
        with self.lock:
            self.strategy_name = name
            self.load_balancer.set_strategy(self._strategy_for(name))
            self.log(f"[LOAD BALANCER] Strategy changed to {name}")

    def toggle_server(self, server_id: str) -> None:
        with self.lock:
            if server_id not in self.servers:
                return
            server = self.servers[server_id]
            server["online"] = not server["online"]
            if server["online"]:
                self.heartbeat_monitor.record_heartbeat(server_id)
                self.failover_manager.update_health(server_id, True, time.time(), "manual recovery")
                self.recovery_manager.mark_recovered(server_id, reason="manual toggle")
                self.recovery_manager.rejoin_cluster(
                    server_id,
                    [peer for peer in SERVER_IDS if peer != server_id],
                )
                if server_id != "Server 1":
                    self.recovery_manager.receive_sync_updates(
                        server_id,
                        self.state_stores["Server 1"].list_seats(),
                    )
                self.log(f"[SERVER] {server_id} toggled online")
            else:
                self.failover_manager.update_health(server_id, False, time.time(), "manual offline")
                self.log(f"[SERVER] {server_id} toggled offline")

    def _current_metrics(self) -> List[ServerMetrics]:
        metrics = []
        for server_id, server in self.servers.items():
            if server["online"]:
                self.heartbeat_monitor.record_heartbeat(server_id)
            breaker = self.breakers[server_id]
            is_online = bool(server["online"]) and breaker.get_state() != CircuitState.OPEN
            metric = ServerMetrics(
                server_id=server_id,
                cpu_usage=float(server["cpu"]),
                active_connections=int(server["connections"]),
                response_time=float(server["response_time"]),
                error_rate=round(breaker.failure_percentage() / 100, 3),
                request_count=int(server["requests"]),
                status="online" if is_online else "offline",
            )
            metrics.append(metric)
        self.metrics_collector.ingest_batch(metrics)
        return metrics

    def _refresh_server_metrics(self) -> None:
        for server in self.servers.values():
            if not server["online"]:
                continue
            server["cpu"] = max(5, min(95, int(server["cpu"]) + random.randint(-5, 6)))
            server["connections"] = max(0, min(30, int(server["connections"]) + random.randint(-1, 2)))
            server["response_time"] = max(25, min(700, int(server["response_time"]) + random.randint(-15, 20)))

    def book_with_retry(self, seat_id: int, requester: str = "Web User") -> Dict[str, object]:
        retry_events = []

        def on_retry(attempt: int, exc: BaseException, delay: float) -> None:
            retry_events.append({"attempt": attempt, "delay": round(delay, 2), "error": str(exc)})
            self.retry_status.update(
                {
                    "count": attempt,
                    "next_delay": round(delay, 2),
                    "stage": f"Attempt {attempt}",
                    "status": f"Retrying: {exc}",
                }
            )
            self.log(f"[RETRY] Booking Seat {seat_id} attempt {attempt}; next retry in {delay:.2f}s")

        def on_giveup(exc: BaseException) -> None:
            self.retry_status["status"] = f"Failed: {exc}"
            self.log(f"[RETRY] Giving up booking Seat {seat_id}: {exc}")

        manager = RetryManager(
            max_attempts=3,
            base_delay=0.25,
            backoff_factor=2,
            max_delay=1.0,
            jitter=True,
            retry_exceptions=(TransientBookingError,),
            on_retry=on_retry,
            on_giveup=on_giveup,
            logger=lambda message: self.log(f"[RETRY] {message}"),
        )

        try:
            result = manager.call(lambda: self._book_once(seat_id, requester))
            self.retry_status.update(
                {
                    "count": len(retry_events),
                    "next_delay": 0.0,
                    "stage": "Complete",
                    "status": "Succeeded",
                }
            )
            result["retries"] = retry_events
            return result
        except NonRetryableBookingError as exc:
            self.retry_status["status"] = f"Blocked: {exc}"
            return self._fail("not_retryable", str(exc), increment=False)
        except RetryError as exc:
            return self._fail("retry_failed", str(exc), increment=False)

    def _book_once(self, seat_id: int, requester: str) -> Dict[str, object]:
        with self.lock:
            return self._book_locked(seat_id, requester)

    def _book_locked(self, seat_id: int, requester: str) -> Dict[str, object]:
        started_at = time.time()
        self._expire_leases_locked()
        self.request_timestamps.append(started_at)

        seat = self.seats.get(seat_id)
        if not seat:
            raise NonRetryableBookingError(f"Seat {seat_id} does not exist")
        if seat["state"] == "reserved":
            raise NonRetryableBookingError(f"Seat {seat_id} is already reserved")
        if seat["state"] == "locked":
            raise NonRetryableBookingError(f"Seat {seat_id} is currently leased")

        metrics = self._current_metrics()
        routing_key = f"seat-{seat_id}"
        selected = self.load_balancer.select(metrics, routing_key=routing_key)
        if not selected:
            raise NonRetryableBookingError("No online server is available")

        selected = self._select_circuit_safe_server(selected, metrics)
        breaker = self.breakers[selected]
        try:
            breaker.call(lambda: self._reserve_on_server(seat_id, requester, selected))
        except CircuitOpenError as exc:
            self.metrics_collector.record_event("circuit_open", str(exc), selected, circuit_state="OPEN")
            raise NonRetryableBookingError(f"Circuit is open for {selected}") from exc
        except LeaseConflictError as exc:
            raise NonRetryableBookingError(str(exc)) from exc
        except TransientBookingError:
            raise
        except Exception as exc:
            raise TransientBookingError(str(exc)) from exc

        server = self.servers[selected]
        server["requests"] = int(server["requests"]) + 1
        self.success_count += 1
        latency_ms = (time.time() - started_at) * 1000
        self.last_route = {
            "algorithm": self.strategy_name,
            "selected_server": selected,
            "routing_key": routing_key,
            "reason": self._routing_reason(metrics, selected),
            "latency_ms": round(latency_ms, 2),
        }
        self.metrics_collector.record_event(
            "routing",
            f"Seat {seat_id} routed to {selected}",
            server_id=selected,
            latency_ms=latency_ms,
            circuit_state=breaker.get_state().value,
        )
        self.log(f"[BOOKING] Seat {seat_id} leased for {requester} through {selected}")
        return {"ok": True, "seat_id": seat_id, "server": selected, "routing_key": routing_key}

    def _select_circuit_safe_server(self, selected: str, metrics: List[ServerMetrics]) -> str:
        breaker = self.breakers[selected]
        if breaker.allow_request():
            return selected
        target = self.failover_manager.choose_failover_target(metrics)
        if not target or target == selected or not self.breakers[target].allow_request():
            raise NonRetryableBookingError(f"Circuit is open for {selected}")
        self.log(f"[FAILOVER] {selected} unavailable; redirecting to {target}")
        return target

    def _reserve_on_server(self, seat_id: int, requester: str, server_id: str) -> None:
        if not self.servers[server_id]["online"]:
            self.breakers[server_id].record_failure()
            raise TransientBookingError(f"{server_id} is offline")

        # Controlled failure injection so the circuit breaker can be observed.
        if random.random() < 0.2:
            raise TransientBookingError(f"{server_id} simulated transient booking failure")

        lease = self.lease_manager.acquire(seat_id, holder=server_id, owner=requester, ttl_seconds=30)
        self.seats[seat_id].update(
            {
                "state": "locked",
                "owner": requester,
                "lease_id": lease.lease_id,
                "expires_at": lease.expires_at,
                "server": server_id,
            }
        )
        updated = self.state_stores["Server 1"].update_seat(seat_id, "locked", owner=requester)
        if updated and self.consistency_simulator:
            self.consistency_simulator.broadcast_with_delay(updated)

    def confirm_seat(self, seat_id: int) -> Dict[str, object]:
        with self.lock:
            seat = self.seats.get(seat_id)
            if not seat or seat["state"] != "locked":
                return self._fail("not_locked", f"Seat {seat_id} is not locked")
            try:
                self.lease_manager.validate(str(seat["lease_id"]), seat_id)
                self.lease_manager.release(str(seat["lease_id"]))
            except LeaseNotFoundError:
                return self._fail("lease_expired", f"Seat {seat_id} lease expired")

            seat.update({"state": "reserved", "lease_id": "", "expires_at": 0.0})
            server_id = str(seat["server"] or "Server 1")
            updated = self.state_stores["Server 1"].update_seat(seat_id, "reserved", owner=str(seat["owner"]))
            if updated and self.consistency_simulator:
                self.consistency_simulator.broadcast_with_delay(updated)
            self.log(f"[BOOKING] Seat {seat_id} confirmed")
            return {"ok": True}

    def cancel_lock(self, seat_id: int) -> Dict[str, object]:
        with self.lock:
            seat = self.seats.get(seat_id)
            if not seat or seat["state"] != "locked":
                return self._fail("not_locked", f"Seat {seat_id} is not locked")
            if seat["lease_id"]:
                self.lease_manager.release(str(seat["lease_id"]))
            seat.update({"state": "available", "owner": "", "lease_id": "", "expires_at": 0.0, "server": ""})
            self.log(f"[BOOKING] Seat {seat_id} lock cancelled")
            return {"ok": True}

    def renew_lease(self, seat_id: int) -> Dict[str, object]:
        with self.lock:
            seat = self.seats.get(seat_id)
            if not seat or seat["state"] != "locked" or not seat["lease_id"]:
                return self._fail("not_locked", f"Seat {seat_id} has no active lease")
            try:
                renewed = self.lease_manager.renew(str(seat["lease_id"]))
            except LeaseNotFoundError as exc:
                return self._fail("lease_expired", str(exc))
            seat["expires_at"] = renewed.expires_at
            self.log(f"[LEASE] Seat {seat_id} lease renewed")
            return {"ok": True, "remaining": renewed.remaining_seconds()}

    def reset(self) -> None:
        with self.lock:
            for seat in self.seats.values():
                seat.update({"state": "available", "owner": "", "lease_id": "", "expires_at": 0.0, "server": ""})
            self.lease_manager = DistributedLeaseManager(default_ttl_seconds=30)
            self.success_count = 0
            self.failure_count = 0
            self.request_timestamps.clear()
            self.last_route = {}
            self.metrics_collector.clear()
            self._initialize_synchronization()
            self.log("[SYSTEM] Seats and metrics reset")

    def simulate_retry(self) -> Dict[str, object]:
        attempts = {"count": 0}

        def flaky_operation() -> str:
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise TransientBookingError("temporary capacity spike")
            return "ok"

        manager = RetryManager(
            max_attempts=4,
            base_delay=0.25,
            backoff_factor=2,
            max_delay=2,
            jitter=True,
            retry_exceptions=(TransientBookingError,),
            on_retry=lambda attempt, exc, delay: self.log(
                f"[RETRY] Attempt {attempt} failed; next retry in {delay:.2f}s"
            ),
            logger=lambda message: self.log(f"[RETRY] {message}"),
        )
        try:
            manager.call(flaky_operation)
            self.retry_status.update(
                {"count": attempts["count"] - 1, "next_delay": 0.0, "stage": "Complete", "status": "Succeeded"}
            )
            return {"ok": True}
        except RetryError as exc:
            return self._fail("retry_failed", str(exc))

    def _expire_leases_locked(self) -> None:
        for lease in self.lease_manager.expire_leases():
            seat = self.seats.get(lease.seat_id)
            if seat and seat["state"] == "locked":
                seat.update({"state": "available", "owner": "", "lease_id": "", "expires_at": 0.0, "server": ""})
                self.log(f"[LEASE] Seat {lease.seat_id} lease expired; returned to available")

    def _on_sync_delivered(self, peer_id, seat, delay_ms) -> None:
        with self.lock:
            self.replicators[peer_id].apply_remote_update(seat, source_server="primary")
            self.log(f"[SYNC] Seat {seat.seat_id} replicated to {peer_id} ({delay_ms}ms)")

    def _apply_recovery_sync_update(self, server_id, seat) -> None:
        with self.lock:
            replicator = self.replicators.get(server_id)
            if not replicator:
                return
            replicator.apply_remote_update(seat, source_server="Server 1")
            self.log(f"[RECOVERY] {server_id} caught up Seat {seat.seat_id}")

    def _routing_reason(self, metrics: List[ServerMetrics], selected: str) -> str:
        by_id = {metric.server_id: metric for metric in metrics}
        metric = by_id.get(selected)
        if self.strategy_name == "Round Robin":
            return "Next online server in rotation"
        if self.strategy_name == "Weighted Round Robin":
            return "Smooth weighted rotation prefers higher-weight servers"
        if self.strategy_name == "Resource-Aware" and metric:
            return (
                f"Lowest resource score: conn={metric.active_connections}, "
                f"cpu={metric.cpu_usage:.0f}%, rt={metric.response_time:.0f}ms"
            )
        if self.strategy_name == "Consistent Hashing":
            return "Stable seat routing key mapped onto the hash ring"
        if self.strategy_name == "Adaptive Feedback" and metric:
            score = 100 - metric.cpu_usage - (metric.active_connections * 2) - (metric.response_time * 0.5) - (metric.error_rate * 5)
            return f"Highest health score ({score:.1f})"
        return "Selected by active load-balancing strategy"

    def _fail(self, code: str, message: str, increment: bool = True) -> Dict[str, object]:
        if increment:
            self.failure_count += 1
        self.metrics_collector.record_event("failure", message)
        self.log(f"[ERROR] {message}")
        return {"ok": False, "error": code, "message": message}

    def log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.logs.append(f"{timestamp} {message}")
        self.logs = self.logs[-150:]

    def snapshot(self) -> Dict[str, object]:
        with self.lock:
            self._expire_leases_locked()
            self._refresh_server_metrics()
            metrics = self._current_metrics()
            now = time.time()
            self.request_timestamps = [ts for ts in self.request_timestamps if now - ts <= 10]
            heartbeat = self.heartbeat_monitor.get_all_server_status()
            summary = self.metrics_collector.summary()
            sync_pending = self.consistency_simulator.get_pending_count() if self.consistency_simulator else 0
            versions = {
                server_id: max((seat.version for seat in store.list_seats()), default=0)
                for server_id, store in self.state_stores.items()
            }
            seats = [
                {
                    **seat,
                    "seat_id": seat_id,
                    "remaining": max(0, int(float(seat["expires_at"]) - now)),
                }
                for seat_id, seat in self.seats.items()
            ]
            return {
                "strategy": self.strategy_name,
                "strategies": list(STRATEGIES),
                "servers": [
                    {
                        **self.servers[metric.server_id],
                        "server_id": metric.server_id,
                        "status": metric.status,
                        "breaker": self.breakers[metric.server_id].get_state().value,
                        "failure_pct": round(self.breakers[metric.server_id].failure_percentage(), 1),
                        "heartbeat": heartbeat.get(metric.server_id, {}),
                        "failover": asdict(self.failover_manager.get_state(metric.server_id)),
                    }
                    for metric in metrics
                ],
                "seats": seats,
                "last_route": self.last_route,
                "retry": self.retry_status,
                "observability": {
                    "rps": round(len(self.request_timestamps) / 10, 2),
                    "success_rate": round((self.success_count / max(1, self.success_count + self.failure_count)) * 100, 1),
                    "failure_rate": round((self.failure_count / max(1, self.success_count + self.failure_count)) * 100, 1),
                    "active_servers": [m.server_id for m in metrics if m.status == "online"],
                    "avg_cpu": summary.average_cpu,
                    "total_connections": summary.total_connections,
                    "avg_response_time": summary.average_response_time,
                },
                "sync": {
                    "pending": sync_pending,
                    "versions": versions,
                    "cluster_members": self.recovery_manager.get_cluster_members(),
                },
                "logs": list(reversed(self.logs[-80:])),
                "telemetry": [event.__dict__ for event in self.metrics_collector.list_events(limit=20)],
            }


app = Flask(__name__)
state = DistributedTicketWebApp()


@app.get("/")
def index():
    return render_template_string(PAGE)


@app.get("/api/state")
def api_state():
    return jsonify(state.snapshot())


@app.post("/api/strategy")
def api_strategy():
    payload = request.get_json(silent=True) or {}
    state.set_strategy(str(payload.get("strategy", "Round Robin")))
    return jsonify({"ok": True})


@app.post("/api/servers/<server_id>/toggle")
def api_toggle_server(server_id):
    state.toggle_server(server_id)
    return jsonify({"ok": True})


@app.post("/api/seats/<int:seat_id>/book")
def api_book_seat(seat_id):
    payload = request.get_json(silent=True) or {}
    requester = str(payload.get("requester", "Web User"))
    return jsonify(state.book_with_retry(seat_id, requester))


@app.post("/api/seats/<int:seat_id>/confirm")
def api_confirm_seat(seat_id):
    return jsonify(state.confirm_seat(seat_id))


@app.post("/api/seats/<int:seat_id>/cancel")
def api_cancel_lock(seat_id):
    return jsonify(state.cancel_lock(seat_id))


@app.post("/api/seats/<int:seat_id>/renew")
def api_renew_lease(seat_id):
    return jsonify(state.renew_lease(seat_id))


@app.post("/api/retry")
def api_retry():
    return jsonify(state.simulate_retry())


@app.post("/api/reset")
def api_reset():
    state.reset()
    return jsonify({"ok": True})


PAGE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Distributed Ticket Booking System</title>
  <style>
    :root {
      --bg: #f6f7f9;
      --panel: #ffffff;
      --ink: #1d2430;
      --muted: #647084;
      --line: #dfe4ea;
      --green: #2f9e44;
      --red: #d64545;
      --amber: #b88700;
      --blue: #276ef1;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--ink);
    }
    header {
      position: sticky;
      top: 0;
      z-index: 2;
      background: var(--panel);
      border-bottom: 1px solid var(--line);
      padding: 16px 24px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }
    main { padding: 20px; max-width: 1480px; margin: 0 auto; }
    h1 { margin: 0; font-size: 22px; }
    h2 { margin: 0 0 12px; font-size: 16px; }
    button, select, input {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fff;
      color: var(--ink);
      padding: 8px 10px;
      font: inherit;
    }
    button { cursor: pointer; }
    button.primary { background: var(--blue); border-color: var(--blue); color: white; }
    button.danger { background: var(--red); border-color: var(--red); color: white; }
    .grid { display: grid; grid-template-columns: repeat(12, minmax(0, 1fr)); gap: 16px; }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 16px;
    }
    .span-3 { grid-column: span 3; }
    .span-4 { grid-column: span 4; }
    .span-6 { grid-column: span 6; }
    .span-8 { grid-column: span 8; }
    .span-12 { grid-column: span 12; }
    .stat { color: var(--muted); font-size: 12px; }
    .stat strong { color: var(--ink); display: block; font-size: 22px; margin-top: 4px; }
    .servers { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; }
    .server, .seat {
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px;
      background: #fbfcfd;
    }
    .server.offline { border-color: var(--red); background: #fff5f5; }
    .seat {
      min-height: 96px;
      text-align: left;
      width: 100%;
    }
    .seat.available { border-color: #b7e4c7; background: #f1fff5; }
    .seat.locked { border-color: #ffe08a; background: #fff9db; }
    .seat.reserved { border-color: #ffc9c9; background: #fff5f5; }
    .seat-grid { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 10px; }
    .muted { color: var(--muted); }
    .pill {
      display: inline-block;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 2px 8px;
      font-size: 12px;
      color: var(--muted);
    }
    .pill.green { color: var(--green); border-color: #b7e4c7; }
    .pill.red { color: var(--red); border-color: #ffc9c9; }
    .row { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
    pre {
      margin: 0;
      white-space: pre-wrap;
      max-height: 360px;
      overflow: auto;
      font-size: 12px;
      line-height: 1.45;
    }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    td, th { border-bottom: 1px solid var(--line); text-align: left; padding: 8px; }
    @media (max-width: 1000px) {
      .span-3, .span-4, .span-6, .span-8 { grid-column: span 12; }
      .servers, .seat-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Distributed Ticket Booking System</h1>
      <div class="muted">Flask web dashboard for browser and Vercel deployment</div>
    </div>
    <div class="row">
      <select id="strategy"></select>
      <button onclick="changeStrategy()">Apply Strategy</button>
      <button onclick="simulateRetry()">Simulate Retry</button>
      <button class="danger" onclick="resetSystem()">Reset</button>
    </div>
  </header>
  <main>
    <section class="grid">
      <div class="panel span-3 stat">Requests/sec<strong id="rps">0</strong></div>
      <div class="panel span-3 stat">Success Rate<strong id="successRate">0%</strong></div>
      <div class="panel span-3 stat">Failure Rate<strong id="failureRate">0%</strong></div>
      <div class="panel span-3 stat">Active Servers<strong id="activeServers">-</strong></div>

      <div class="panel span-8">
        <h2>Server Cluster</h2>
        <div class="servers" id="servers"></div>
      </div>
      <div class="panel span-4">
        <h2>Load Balancer Decision Details</h2>
        <div id="decision" class="muted">No routing decision yet.</div>
      </div>

      <div class="panel span-8">
        <h2>Seat Reservation</h2>
        <div class="seat-grid" id="seats"></div>
      </div>
      <div class="panel span-4">
        <h2>Synchronization / Recovery</h2>
        <table>
          <tbody id="sync"></tbody>
        </table>
        <h2 style="margin-top:16px;">Retry State</h2>
        <div id="retry" class="muted"></div>
      </div>

      <div class="panel span-6">
        <h2>Telemetry Events</h2>
        <pre id="telemetry"></pre>
      </div>
      <div class="panel span-6">
        <h2>Event Log</h2>
        <pre id="logs"></pre>
      </div>
    </section>
  </main>
  <script>
    let state = null;

    async function api(path, options = {}) {
      const response = await fetch(path, {
        headers: { "Content-Type": "application/json" },
        ...options,
      });
      return response.json();
    }

    async function loadState() {
      state = await api("/api/state");
      render();
    }

    function render() {
      document.getElementById("rps").textContent = state.observability.rps;
      document.getElementById("successRate").textContent = state.observability.success_rate + "%";
      document.getElementById("failureRate").textContent = state.observability.failure_rate + "%";
      document.getElementById("activeServers").textContent = state.observability.active_servers.join(", ") || "None";

      const strategy = document.getElementById("strategy");
      strategy.innerHTML = state.strategies.map(name => `<option ${name === state.strategy ? "selected" : ""}>${name}</option>`).join("");

      document.getElementById("servers").innerHTML = state.servers.map(server => `
        <div class="server ${server.online ? "" : "offline"}">
          <div class="row" style="justify-content:space-between;">
            <strong>${server.server_id}</strong>
            <span class="pill ${server.status === "online" ? "green" : "red"}">${server.status}</span>
          </div>
          <div class="muted">CPU ${server.cpu}% · Conn ${server.connections} · RT ${server.response_time}ms</div>
          <div class="muted">Requests ${server.requests} · CB ${server.breaker} · Fail ${server.failure_pct}%</div>
          <div class="muted">Heartbeat ${server.heartbeat.is_alive ? "UP" : "DOWN"}</div>
          <button onclick="toggleServer('${server.server_id}')">${server.online ? "Take Offline" : "Recover"}</button>
        </div>
      `).join("");

      document.getElementById("seats").innerHTML = state.seats.map(seat => `
        <button class="seat ${seat.state}" onclick="bookSeat(${seat.seat_id})">
          <strong>Seat ${seat.seat_id}</strong><br>
          State: ${seat.state}<br>
          <span class="muted">${seat.owner ? "Owner: " + seat.owner + "<br>" : ""}${seat.server ? "Server: " + seat.server + "<br>" : ""}${seat.remaining ? "Lease TTL: " + seat.remaining + "s" : ""}</span>
          <div class="row" style="margin-top:8px;">
            ${seat.state === "locked" ? `<span onclick="event.stopPropagation(); confirmSeat(${seat.seat_id})" class="pill green">Confirm</span><span onclick="event.stopPropagation(); cancelSeat(${seat.seat_id})" class="pill red">Cancel</span><span onclick="event.stopPropagation(); renewSeat(${seat.seat_id})" class="pill">Renew</span>` : ""}
          </div>
        </button>
      `).join("");

      const route = state.last_route || {};
      document.getElementById("decision").innerHTML = route.selected_server ? `
        <p><strong>Algorithm:</strong> ${route.algorithm}</p>
        <p><strong>Selected Server:</strong> ${route.selected_server}</p>
        <p><strong>Routing Key:</strong> ${route.routing_key}</p>
        <p><strong>Reason:</strong> ${route.reason}</p>
        <p><strong>Latency:</strong> ${route.latency_ms || 0}ms</p>
      ` : "No routing decision yet.";

      document.getElementById("sync").innerHTML = Object.entries(state.sync.versions).map(([server, version]) => `
        <tr><th>${server}</th><td>Version ${version}</td></tr>
      `).join("") + `<tr><th>Pending Replication</th><td>${state.sync.pending}</td></tr><tr><th>Cluster Members</th><td>${state.sync.cluster_members.join(", ") || "-"}</td></tr>`;

      document.getElementById("retry").textContent = `${state.retry.status} · count=${state.retry.count} · next=${state.retry.next_delay}s`;
      document.getElementById("logs").textContent = state.logs.join("\\n");
      document.getElementById("telemetry").textContent = state.telemetry.map(event => `${event.timestamp} ${event.event_type} ${event.server_id || ""} ${event.message}`).join("\\n");
    }

    async function changeStrategy() {
      await api("/api/strategy", { method: "POST", body: JSON.stringify({ strategy: document.getElementById("strategy").value }) });
      await loadState();
    }
    async function toggleServer(serverId) {
      await api(`/api/servers/${encodeURIComponent(serverId)}/toggle`, { method: "POST" });
      await loadState();
    }
    async function bookSeat(seatId) {
      const seat = state.seats.find(item => item.seat_id === seatId);
      if (!seat || seat.state !== "available") return;
      await api(`/api/seats/${seatId}/book`, { method: "POST", body: JSON.stringify({ requester: "Web User" }) });
      await loadState();
    }
    async function confirmSeat(seatId) {
      await api(`/api/seats/${seatId}/confirm`, { method: "POST" });
      await loadState();
    }
    async function cancelSeat(seatId) {
      await api(`/api/seats/${seatId}/cancel`, { method: "POST" });
      await loadState();
    }
    async function renewSeat(seatId) {
      await api(`/api/seats/${seatId}/renew`, { method: "POST" });
      await loadState();
    }
    async function simulateRetry() {
      await api("/api/retry", { method: "POST" });
      await loadState();
    }
    async function resetSystem() {
      await api("/api/reset", { method: "POST" });
      await loadState();
    }

    loadState();
    setInterval(loadState, 4000);
  </script>
</body>
</html>
"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
