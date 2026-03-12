# موارد مهم و تاییدشده Nexora

این فایل فقط شامل موارد دقیق و مهم است که با کد فعلی پروژه و لاگ‌های عملیاتی هم‌خوانی دارند.

## 1) mismatch بحرانی بین easy-run و systemd (سمت inside)

- `easy-run` فایل `/etc/default/nexora-client-forward` را می‌نویسد.
- اما یونیت `nexora-client-forward.service` این فایل را لود نمی‌کند (نداشتن `EnvironmentFile`).
- نتیجه: بخشی از تنظیمات عملیاتی (مثل `max_conns` و پارامترهای poll/timeout) ممکن است اعمال نشوند.

مراجع:
- `deploy/easy-run.sh` (بخش `install_client`)
- `deploy/systemd/nexora-client-forward.service`
- `docs/SYSTEMD_QUICKSTART.md` (دستورهای وابسته به `/etc/default/nexora-client-forward`)

## 2) پارامتر dead config: `forward-max-conns-per-ip`

- آرگومان `--forward-max-conns-per-ip` در CLI تعریف و پاس داده می‌شود.
- ولی در مسیر اجرای `run_forward_server` هیچ reject بر اساس این limit انجام نمی‌شود.
- نتیجه: اپراتور فکر می‌کند محدودیت per-IP فعال است، اما عملا enforce نمی‌شود.

مراجع:
- `src/nexora_client.py` (تعریف آرگومان)
- `src/nexora_client.py` (`run_forward_server`، فقط شمارش `ip_counts`)

## 3) نبود authentication/encryption در لایه پروتکل

- در تبادل packet بین client/server، MAC یا امضای رمزنگاری‌شده وجود ندارد.
- ترافیک تونل plaintext است و از دید resolver/intermediate قابل مشاهده است.

مراجع:
- `src/nexora_client.py`
- `src/nexora_server.py`
- `src/nexora_proto.py`

## 4) ریسک data loss خاموش در `SEQ_MAP_MAX_SIZE`

- در کلاینت، با بزرگ شدن `seq_map`، آیتم‌های قدیمی بدون سیگنال پروتکلی حذف می‌شوند.
- در شرایط out-of-order سنگین، این می‌تواند به از دست رفتن بخشی از downstream منجر شود.

مراجع:
- `src/nexora_client.py` (`SEQ_MAP_MAX_SIZE` و trimming روی `seq_map`)

## 5) احتمال false-positive در zombie detection

- اگر ظرف حدود 4 ثانیه اول داده downstream نرسد، stream بسته می‌شود.
- در شبکه‌های پر jitter یا backend کند، ممکن است اتصال سالم زودتر از موعد قطع شود.

مراجع:
- `src/nexora_client.py` (شرط `now - session_start >= 4.0`)

## 6) وابستگی شدید به کیفیت `resolver_file`

- وقتی `resolver_file` معتبر و non-empty باشد، لیست resolverها جایگزین ورودی CLI می‌شود.
- اگر فایل کیفیت پایین داشته باشد (resolverهای NXDOMAIN/timeout)، پایداری تونل افت می‌کند.

مراجع:
- `src/nexora_client.py` (مسیر load از `--resolver-file`)

## اولویت اقدام پیشنهادی

1. رفع mismatch یونیت کلاینت و فعال‌سازی واقعی `EnvironmentFile`.
2. تعیین تکلیف `forward-max-conns-per-ip`:
   - یا enforce واقعی
   - یا حذف کامل از CLI/docs تا ابهام از بین برود.
3. بازتنظیم محافظه‌کارانه zombie detection (افزایش آستانه یا شرط ترکیبی).
4. تثبیت سیاست resolver-file (fallback امن به seed resolverها در شرایط بحرانی).

