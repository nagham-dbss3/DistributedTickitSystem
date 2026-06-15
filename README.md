# Distributed Ticket Booking System

واجهة Tkinter لمحاكاة نظام حجز تذاكر موزع. المشروع يعرض سلوك عدة مكونات مع بعض:

- Server Cluster
- API Gateway
- Load Balancer strategies
- Heartbeat monitoring
- Failure detection
- Circuit Breaker
- Retry dashboard
- Failover dashboard
- Cluster topology visualization
- Real-time observability dashboard
- Seat reservation grid
- Synchronization dashboard
- Event log

## المتطلبات

- Python 3.x
- Tkinter، غالبا يكون مرفق مع Python على Windows
- لا يوجد dependencies خارجية مطلوبة حاليا

## تشغيل النظام

من جذر المشروع:

```powershell
python dashboard.py
```

إذا فتحت الواجهة وما ظهر كل شيء، استخدم عجلة الماوس. الداشبورد صارت قابلة للتمرير عموديا.

## كيف تتحقق من النظام من الواجهة

### 1. Server Cluster Panel

المفروض يظهر 3 سيرفرات:

- Server 1
- Server 2
- Server 3

كل سيرفر يعرض:

- CPU
- Connections
- Requests
- CB State
- Failures
- Error %
- Cooldown

المتوقع:

- الحالة تبدأ Online، والدائرة تكون خضراء.
- CPU وConnections وRequests تتغير تلقائيا كل ثانية.
- زر `Toggle` يحول السيرفر بين Online وOffline.

اختبار سريع:

1. اضغط `Toggle` على Server 2.
2. يجب أن يتوقف عن إرسال heartbeat.
3. بعد مهلة قصيرة يظهر أنه Down في Heartbeat Monitor.
4. عند إعادة الضغط على `Toggle` يرجع Online.

### 2. Heartbeat Monitor

يعرض لكل سيرفر:

- آخر heartbeat time
- الزمن منذ آخر heartbeat بالميلي ثانية
- الحالة UP أو DOWN

المتوقع:

- السيرفرات Online تظهر `UP`.
- عند إيقاف سيرفر بزر `Toggle`، بعد timeout تظهر `DOWN`.
- Event Log يسجل رسائل مثل:

```text
[FAILURE DETECTOR] Server Server 2 heartbeat timeout
[FAILURE DETECTOR] Server Server 2 recovered
```

### 3. Retry Dashboard

يعرض:

- Retry Count
- Next Retry Delay
- Backoff Stage
- Retry Status

اختبار:

1. اضغط `Simulate Retry`.
2. يجب أن ترى عدة محاولات Retry.
3. `Retry Count` و`Backoff Stage` يزيدان.
4. `Next Retry Delay` يعرض التأخير القادم.
5. في النهاية تظهر الحالة `Succeeded` أو `Failed`.

### 4. API Gateway & Load Balancer Control

يوجد اختيار لخوارزمية Load Balancing:

- Round Robin
- Weighted Round Robin
- Resource-Aware
- Consistent Hashing
- Adaptive Feedback

اختبار:

1. اختر خوارزمية من القائمة.
2. اضغط على مقعد Available في Seat Reservation Panel.
3. يجب أن يظهر `Selected Server`.
4. يجب أن يظهر سبب التوجيه في `Routing Reason`.
5. Event Log يجب أن يسجل قرار التوجيه.

المتوقع حسب الخوارزمية:

- `Round Robin`: يوزع الطلبات بالتناوب على السيرفرات.
- `Weighted Round Robin`: يعطي Server 1 وزنا أعلى من Server 2 وServer 3.
- `Resource-Aware`: يختار السيرفر الأقل حملا حسب connections وCPU وresponse time.
- `Consistent Hashing`: يوجه حسب routing key ويقلل إعادة التوزيع.
- `Adaptive Feedback`: يختار أعلى health score اعتمادا على telemetry.

### 5. Cluster Topology

يعرض المسار:

```text
Client
  |
Gateway
  |
Load Balancer
  |
Servers
```

اختبار:

1. اضغط على أي مقعد متاح.
2. يجب أن يتم تلوين المسار النشط:
   `Client -> Gateway -> Load Balancer -> Selected Server`
3. إذا كان السيرفر Offline يظهر بلون تحذيري.
4. عند تغيير load balancer strategy يجب أن يتغير اسم الاستراتيجية داخل عقدة Load Balancer.

### 6. Failover Dashboard

يعرض:

- Current Primary Server
- Active Failovers
- Recovery Events

اختبار:

1. أوقف السيرفر الذي يتم اختياره حاليا باستخدام `Toggle`.
2. احجز مقعدا جديدا.
3. يجب أن يحاول Gateway التوجيه إلى سيرفر صحي آخر.
4. Event Log يجب أن يعرض رسائل Failover مثل:

```text
[FAILOVER]
Server Server 2 unavailable
Redirecting traffic to Server 1
```

### 7. Real-Time Observability Dashboard

يعرض:

- Requests Per Second
- Success Rate
- Failure Rate
- Current Load Balancer
- Current Circuit Breaker State
- Active Servers

اختبار:

1. اضغط عدة مقاعد بسرعة.
2. `Requests Per Second` يجب أن يرتفع مؤقتا.
3. `Success Rate` يجب أن يبقى مرتفعا عندما الطلبات تنجح.
4. إذا أوقفت كل السيرفرات وحاولت الحجز، `Failure Rate` يجب أن يزيد.
5. `Current Load Balancer` يجب أن يطابق الاستراتيجية المختارة.
6. `Active Servers` يجب أن يعرض السيرفرات Online فقط والتي Circuit Breaker عندها ليس OPEN.

### 8. Seat Reservation Panel

يعرض 20 مقعدا.

حالات المقعد:

- Available
- Locked
- Reserved

اختبار:

1. اضغط على مقعد Available.
2. يجب أن ينتقل إلى Locked مع عداد.
3. بعد انتهاء العداد يتحول إلى Reserved.
4. زر `Reset Seats` يعيد المقاعد إلى Available.

### 9. Synchronization Dashboard

يعرض مؤشرات replication/synchronization:

- Last Sync Event
- Sync Delay
- Pending Repl
- Repl Status
- Server versions
- Max Lag

ملاحظة: هذا القسم جاهز للعرض والتكامل مع منطق synchronization، وبعض القيم قد تبقى افتراضية إذا لم يتم استدعاء دوال sync من سيناريوهات الحجز.

### 10. Event Log

يسجل الأحداث المهمة:

- اختيار Load Balancer
- telemetry
- routing decisions
- heartbeat failures
- recovery
- retry events
- failover events
- seat state changes

اختبار:

1. احجز مقعدا.
2. غير الاستراتيجية.
3. أوقف سيرفر.
4. شغل Retry simulation.
5. يجب أن تظهر الأحداث كلها في Event Log.

## اختبار شامل يدوي للنظام

اتبع هذا السيناريو من البداية للنهاية:

1. شغل الواجهة:

```powershell
python dashboard.py
```

2. تأكد أن السيرفرات الثلاثة Online وHeartbeat Monitor يظهر `UP`.

3. جرّب كل Load Balancer strategy:

- اختر الاستراتيجية.
- احجز مقعدين أو ثلاثة.
- راقب Selected Server وEvent Log وCluster Topology.

4. اختبر Failure Detection:

- اضغط `Toggle` على Server 2.
- انتظر حتى يظهر DOWN.
- راقب Event Log وFailover Dashboard.

5. اختبر Failover:

- مع Server 2 Offline، احجز مقعدا جديدا.
- يجب أن يتم اختيار سيرفر Online آخر.

6. اختبر Observability:

- اضغط عدة مقاعد بسرعة.
- راقب RPS وSuccess Rate وActive Servers.

7. اختبر فشل الطلبات:

- أوقف كل السيرفرات.
- حاول حجز مقعد.
- يجب أن يظهر في Event Log أنه لا يوجد online servers.
- Failure Rate يجب أن يزيد.

8. أعد السيرفرات Online:

- اضغط `Toggle` لكل سيرفر Offline.
- انتظر recovery.
- Heartbeat Monitor يجب أن يرجع `UP`.
- Active Servers يجب أن يعرض السيرفرات.

9. اختبر Retry:

- اضغط `Simulate Retry`.
- راقب Retry Count وNext Retry Delay وEvent Log.

10. اختبر Reset:

- اضغط `Reset Seats`.
- يجب أن ترجع المقاعد غير المحجوزة إلى Available.

## اختبار الكود من الطرفية

### فحص syntax لكل ملفات Python

PowerShell:

```powershell
Get-ChildItem -Recurse -Filter *.py | Where-Object { $_.FullName -notlike "*\venv\*" } | ForEach-Object { python -m py_compile $_.FullName }
```

إذا لم يظهر output فهذا يعني أن الفحص نجح.

### فحص استيراد أهم الموديولات

```powershell
python -c "import dashboard; print('dashboard ok')"
python -c "from load_balancer import LoadBalancer, ServerMetrics; print('load_balancer ok')"
python -c "from monitoring import HeartbeatMonitor, FailureDetector, HealthChecker; print('monitoring ok')"
python -c "from circuit_breaker import Breaker, CircuitState; print('circuit_breaker ok')"
python -c "from retry.retry_manager import RetryManager; print('retry ok')"
python -c "from failover.failover_manager import FailoverManager; print('failover ok')"
```

المتوقع:

```text
dashboard ok
load_balancer ok
monitoring ok
circuit_breaker ok
retry ok
failover ok
```

### اختبار HealthChecker سريع

```powershell
python -c "from monitoring import HealthChecker, ServerState; now=[0.0]; hc=HealthChecker(degraded_after_sec=5, offline_after_sec=10, time_fn=lambda: now[0]); hc.register_server('s1'); assert hc.get_state('s1') is ServerState.OFFLINE; hc.record_heartbeat('s1'); assert hc.get_state('s1') is ServerState.ONLINE; now[0]=6; assert hc.get_state('s1') is ServerState.DEGRADED; now[0]=11; assert hc.get_state('s1') is ServerState.OFFLINE; print('health checker ok')"
```

المتوقع:

```text
health checker ok
```

### اختبار Load Balancer سريع

```powershell
python -c "from load_balancer import LoadBalancer, RoundRobinStrategy, ServerMetrics; metrics=[ServerMetrics('s1',10,1,100,0,1,'online'), ServerMetrics('s2',20,1,100,0,1,'online')]; lb=LoadBalancer(RoundRobinStrategy()); print(lb.select(metrics)); print(lb.select(metrics))"
```

المتوقع أن يطبع سيرفرين بالتناوب، مثلا:

```text
s1
s2
```

## المشاكل المتوقعة وحلولها

### الواجهة لا تظهر كاملة

استخدم عجلة الماوس للتمرير. إذا كانت الشاشة صغيرة، كبّر النافذة أو استخدم scroll.

### Tkinter غير موجود

تأكد أن Python مثبت مع Tkinter. على Windows غالبا يكون موجودا تلقائيا مع Python الرسمي.

### لا تظهر تغييرات Failure Detector بسرعة

Heartbeat timeout مضبوط تقريبا على 15 ثانية، لذلك انتظر قليلا بعد إيقاف السيرفر.

### كل الطلبات تفشل

تأكد أن هناك سيرفر واحد على الأقل Online وأن Circuit Breaker ليس OPEN لكل السيرفرات.

## هيكل المشروع

```text
dashboard.py
load_balancer/
monitoring/
circuit_breaker/
retry/
failover/
synchronization/
telemetry/
```

## ملاحظات

- هذا المشروع يحاكي النظام داخل GUI واحدة، وليس backend موزع حقيقي.
- الهدف هو إظهار سلوك مكونات الأنظمة الموزعة بطريقة مرئية وقابلة للتجربة.
- Event Log هو أفضل مكان للتأكد من تسلسل الأحداث والقرارات.
