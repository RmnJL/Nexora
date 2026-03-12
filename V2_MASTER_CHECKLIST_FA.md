# چک لیست Master برای Backup v1 و شروع Nexora v2

**تاریخ:** 2026-03-12  
**مالک:** RmnJL  
**هدف:** قفل نسخه پایدار v1 به عنوان Backup رسمی روی Git و شروع v2 با طرح حرفه ای و قابل پیگیری.

---

## 1) وضعیت فعلی v1 (واقعی و خلاصه)

- مسیر تونل کار می کند و ترافیک واقعی روی موبایل مشاهده شده است.
- در سناریوهای کوتاه، throughput اولیه خوب بوده (نمونه: چند کیلوبایت اول سریع).
- مشکل اصلی هنوز پایداری در بار واقعی است:
  - `forward timeout` زیاد
  - `sid=None` در handshake/open
  - `bcast_fail` و `fallback_fail` بالا
  - افت کیفیت در multi-user

**نتیجه:** v1 برای آزمایش فنی مناسب است ولی برای Production چندکاربره، نیازمند v2 است.

---

## 2) چک لیست Release/Backup v1 روی Git

### 2.1 قفل وضعیت کد
- [x] `main` تمیز و بدون تغییر local بررسی شد.
- [x] تست کامل پروژه پاس شد (`73 passed`).
- [x] آخرین Fix های پایداری در `main` push شد.

### 2.2 نسخه Backup رسمی
- [x] چک لیست رسمی v2 در مخزن اضافه شد (همین فایل).
- [x] فایل Release Note نهایی v1 تکمیل شد (فقط خلاصه عملیاتی، بدون تحلیل اضافی).
- [x] Tag رسمی Backup v1 ایجاد شد: `v1-backup-2026-03-12`.
- [x] Tag به `origin` push شد.
- [x] لینک Tag به عنوان مبنای شروع v2 ثبت شد.

### 2.3 معیار پذیرش این فاز
- [x] با یک دستور `git checkout <tag>` می توان دقیقا v1 را بازگردانی کرد.
- [x] نقطه مرجع ثابت قبل از v2 روی Git ثبت شد.

---

## 3) تصمیم معماری v2 (مصوب برای شروع)

**تصمیم کلیدی:** مهاجرت از مدل `per-connection DNS session` به مدل `Carrier Tunnel + Multiplex`.

- [ ] Carrier Session بلندمدت (1 تا 3 session پایدار) بین inside/outside.
- [ ] Multiplex چند stream کاربر روی carrier مشترک با `stream_id`.
- [ ] مکانیزم `ACK range + retransmit window` برای تحمل loss.
- [ ] FEC سبک (Parity) برای کاهش impact packet loss.
- [ ] Cohort Resolver پایدار (sticky pool) به جای churn زیاد.
- [ ] Health + failover مرحله ای (نه سوئیچ تهاجمی در هر خطا).

### معیار پذیرش معماری
- [ ] طراحی پیام ها و state machine مکتوب و قابل پیاده سازی باشد.
- [ ] هیچ ambiguity در lifecycle stream/carry وجود نداشته باشد.

---

## 4) چک لیست طراحی فنی v2

### 4.1 Protocol Spec
- [ ] تعریف Frame Header v2 (version, carrier_id, stream_id, flags, seq, ack, window, payload_len).
- [ ] تعریف پیام های Control (`OPEN`, `OPEN_ACK`, `CLOSE`, `PING`, `PONG`, `RESET`).
- [ ] تعریف پیام های Data (`DATA`, `DATA_ACK`, `RETX_REQUEST`).
- [ ] تعریف سیاست reorder و duplicate handling.
- [ ] تعریف timeout/retry دقیق در سطح carrier (نه فقط query).

### 4.2 Flow Control
- [ ] Sliding Window برای هر stream.
- [ ] Backpressure از downstream به upstream.
- [ ] محدودیت حافظه در receive buffer per-stream.
- [ ] سیاست drop و recovery تحت فشار.

### 4.3 Reliability
- [ ] Retransmission policy (RTO adaptive).
- [ ] FEC policy (نرخ parity، اندازه بلوک).
- [ ] Session resume بعد از قطع کوتاه carrier.
- [ ] Circuit breaker برای resolver های ناسالم.

### 4.4 Security/Hardening
- [ ] nonce/replay protection در سطح frame.
- [ ] سقف منابع: stream/session/buffer.
- [ ] safe defaults برای multi-user abuse.

---

## 5) چک لیست پیاده سازی v2

### 5.1 Inside (client-forward)
- [ ] `CarrierManager` پیاده سازی شود.
- [ ] `StreamMux` برای map کردن اتصال کاربر به `stream_id`.
- [ ] صف خروجی/ورودی با اولویت بندی کنترل/داده.
- [ ] health loop و resolver cohort manager جدید.

### 5.2 Outside (nexora-server)
- [ ] `CarrierSessionStore` با TTL و cleanup ایمن.
- [ ] stream table + target socket map.
- [ ] بسته بندی پاسخ چند stream در carrier response.
- [ ] retransmit cache به اندازه کافی کوچک و موثر.

### 5.3 سازگاری
- [ ] feature flag برای اجرای v1/v2 کنار هم.
- [ ] fallback کنترل شده به v1 تا زمان تثبیت v2.

---

## 6) چک لیست تست و کیفیت v2

### 6.1 Unit
- [ ] parser/packer frame v2
- [ ] reorder/retransmit logic
- [ ] flow-control window math
- [ ] error paths (timeout/reset/partial loss)

### 6.2 Integration
- [ ] inside + outside با packet loss مصنوعی
- [ ] latency/jitter مصنوعی
- [ ] stream churn (open/close سریع)
- [ ] long-lived stream stability

### 6.3 Load
- [ ] تست 1 کاربر، 5 کاربر، 20 کاربر
- [ ] KPI: success rate, p50, p95, p99, timeout rate
- [ ] مقایسه مستقیم v1 vs v2

### 6.4 Acceptance Gate
- [ ] success_rate >= 95% در بار هدف
- [ ] کاهش محسوس `sid=None` و `forward timeout`
- [ ] عدم افت شدید throughput پس از 30 ثانیه

---

## 7) چک لیست Rollout عملیاتی

- [ ] Stage-0: آزمایش dev با traffic مصنوعی
- [ ] Stage-1: Canary واقعی (1 کاربر)
- [ ] Stage-2: Canary محدود (5 کاربر)
- [ ] Stage-3: Rollout تدریجی
- [ ] Rollback یک مرحله ای به tag Backup v1

### معیار توقف (Stop Conditions)
- [ ] افزایش timeout بیش از 20% نسبت به baseline
- [ ] افزایش fail rate بیش از 10% پایدار
- [ ] memory leak یا saturation منابع

---

## 8) خروجی های اجباری قبل از شروع کدنویسی v2

- [x] `docs/V2_PROTOCOL_SPEC.md`
- [x] `docs/V2_FLOW_STATE_MACHINE.md`
- [x] `docs/V2_TEST_PLAN.md`
- [x] `docs/V2_ROLLOUT_PLAN.md`

---

## 9) تعریف Done برای v2 (Definition of Done)

- [ ] حداقل 7 روز پایداری در محیط واقعی
- [ ] KPI تایید شده برای چند کاربر
- [ ] rollback test انجام شده و موفق
- [ ] مستندات عملیات (runbook) کامل
- [ ] Tag رسمی Release v2 ساخته شده
