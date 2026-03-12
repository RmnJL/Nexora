# چک‌لیست اجرای Broadcast/Multipath در Nexora

**تاریخ:** 2026-03-12  
**وضعیت:** بازبینی شد (اجرای بخشی)  
**امضا:** RmnJL

---

## 0) قوانین اجرا (قبل از کدنویسی)
- [ ] هدف فاز اول را نهایی کنیم: `10 DNS` همزمان در مسیر ارسال/دریافت.
- [ ] تعریف کنیم Broadcast برای کدام پیام‌ها فعال باشد:
  - [ ] فقط `TYPE_STREAM_SEND`
  - [ ] `TYPE_DATA`
  - [ ] `HELLO` (با اعتبارسنجی متفاوت handshake)
- [x] سیاست ترافیکی را قطعی کنیم:
  - [x] `always-on` برای همه queryها
  - [ ] `adaptive` (بر اساس کیفیت resolver)
- [ ] SLO اولیه را ثبت کنیم:
  - [ ] موفقیت query >= `99%`
  - [ ] افت throughput کمتر از `20%`
  - [ ] افزایش latency p95 کمتر از `30%`

---

## 1) فاز پایدارسازی پایه (Blockerها)
- [x] enforce واقعی `--forward-max-conns-per-ip` در `run_forward_server`.
- [x] اصلاح `max_inflight_per_resolver` تا روی resolver فعال هم واقعا اعمال شود.
- [x] اصلاح رفتار race در پردازش `done_set` تا success همزمان از دست نرود.
- [x] تنظیم قابل کانفیگ برای zombie detection (به‌جای ثابت 4 ثانیه).
- [x] اضافه‌کردن log ساخت‌یافته برای retry/backoff (metric-ready).

**پذیرش فاز 1**
- [x] تست‌های فعلی هسته پاس باشند: `68 passed`.
- [ ] هیچ رگرسیون در handshake/data/stream رخ ندهد.

---

## 2) فاز Broadcast Core (قلب ایده)
- [ ] پیاده‌سازی `BroadcastQueryManager` production-grade (نه نسخه دمو).
- [x] انتخاب resolverها با اولویت کیفیت واقعی selector (نه random ساده).
- [x] افزودن پارامترها:
  - [x] `--broadcast-enable/disable`
  - [x] `--broadcast-fanout` (پیش‌فرض 10)
  - [x] `--broadcast-timeout`
  - [x] `--broadcast-per-resolver-timeout`
- [x] پیاده‌سازی `first valid response wins` با cancel صحیح losers.
- [x] اتصال به `ResolverSelector.report_success/failure` برای هر resolver شرکت‌کننده.
- [ ] طراحی استراتژی fallback:
  - [x] اگر broadcast fail شد -> مسیر serial فعلی
  - [x] اگر بخشی fail شد -> ادامه با پاسخ winner

**پذیرش فاز 2**
- [ ] queryهای استریم با fanout=10 بدون crash کار کنند.
- [ ] success-rate نسبت به serial baseline رشد معنادار داشته باشد.

---

## 3) فاز سازگاری پروتکل و «داده نام‌دار»
- [x] تثبیت idempotency بر پایه `(msg_type, session_id, nonce)`.
- [x] برای `HELLO` قانون جداگانه اعتبارسنجی تعریف شود:
  - [x] nonce باید match شود
  - [x] session_id نباید hardcode/پیش‌فرض فرض شود
- [x] بررسی دوباره dedup cache سرور برای queryهای broadcast همزمان.
- [x] بررسی safety در `TYPE_STREAM_SEND` تا side effect تکراری ایجاد نشود.
- [ ] اضافه‌کردن تست replay/retry با nonce یکسان.

**پذیرش فاز 3**
- [ ] handshake در حالت broadcast و non-broadcast پایدار باشد.
- [ ] هیچ duplicate side-effect در backend مشاهده نشود.

---

## 4) فاز تست و کیفیت
- [ ] اصلاح کامل `tests/test_broadcast_redundancy.py`:
  - [x] importهای ناقص
  - [x] فرمول‌های ریاضی اشتباه
  - [ ] benchmarkهای غیرواقعی
- [ ] افزودن unit testهای جدید:
  - [ ] fanout=10 success/failure mix
  - [ ] cancel path
  - [x] fallback to serial
  - [x] nonce/session validation
  - [x] inflight guard behavior
- [ ] افزودن integration test:
  - [ ] stream send/recv زیر loss مصنوعی
  - [ ] resolver degrade و جایگزینی لحظه‌ای
- [x] اجرای suite کامل:
  - [x] `pytest -q` باید سبز کامل باشد.

---

## 5) فاز Observability و KPI
- [ ] افزودن شمارنده‌ها:
  - [ ] `broadcast_queries_total`
  - [x] `broadcast_success_total`
  - [x] `broadcast_fail_total`
  - [ ] `broadcast_fallback_total`
  - [x] `resolver_switch_total`
- [x] افزودن latency distribution:
  - [x] p50 / p95 / p99 query latency
- [x] افزودن گزارش دوره‌ای KPI در لاگ (هر 60 ثانیه).
- [ ] استخراج baseline قبل/بعد و ثبت delta.

**پذیرش فاز 5**
- [ ] بتوانیم با log تنها، موفقیت 99% را اثبات کنیم.

---

## 6) فاز استقرار مرحله‌ای
- [ ] rollout روی staging با `fanout=3`.
- [ ] تست پایداری 30 دقیقه‌ای و بررسی KPI.
- [ ] افزایش fanout به `5` و تکرار KPI.
- [ ] افزایش fanout به `10` و تکرار KPI.
- [ ] اگر KPI افت کرد:
  - [ ] fallback فوری به fanout قبلی
  - [ ] بررسی resolver pool و timeoutها
- [ ] بعد از تایید، canary production:
  - [ ] 10% -> 50% -> 100%

---

## 7) تنظیمات پیشنهادی شروع (نسخه اول)
- [x] `broadcast_fanout=10`
- [x] `broadcast_timeout=2.8s`
- [x] `broadcast_per_resolver_timeout=1.2s`
- [x] `resolver_max_inflight=1` (تا بعد از KPI اولیه)
- [x] `resolver_attempt_cap=6` (با بازنگری بعد از KPI)

---

## 8) Definition of Done (Done واقعی)
- [x] کل تست‌ها سبز.
- [ ] KPI واقعی محیط اجرا ثبت و ضمیمه.
- [ ] success-rate عملیاتی >= 99% در بازه تست توافق‌شده.
- [ ] rollback plan مستند و تست‌شده.
- [ ] مستندات و سرویس systemd sync با پارامترهای جدید.
- [ ] امضای نهایی تغییرات: **RmnJL**

---

## Work Log
- [x] 2026-03-12: ایجاد چک‌لیست اجرایی broadcast/multipath. (RmnJL)
- [x] 2026-03-12: Checklist reviewed and completed items were checked against current code/tests. (Codex)
- [x] 2026-03-12: بازبینی عمیق پس از تغییرات جدید؛ آیتم‌های دارای شواهد مستقیم در کد/تست تیک شد. (Codex)
- [x] 2026-03-12: فیکس fallback در حالت fanout=pool + افزودن تست رگرسیون + sync مستندات CLI. (Codex)
