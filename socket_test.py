#!/usr/bin/env python3
"""
Замер времени установки WebSocket-соединения (без отправки ордеров).

Для каждого рынка меряется ОТКРЫТИЕ СОКЕТА: DNS + TCP + TLS + WS-upgrade.
Именно эта величина в основном бенчмарке (latency_test.py) вынесена ЗА
таймер как разовая стоимость соединения.

Дополнительно для OKX меряется ЛОГИН (op: login) на уже открытом сокете -
у OKX без логина торговать нельзя. У Binance отдельного логина нет: каждый
запрос подписывается индивидуально, поэтому открытый сокет = готов к ордерам.

Ключи OKX (для логина) берутся из .env, как и в latency_test.py:
    OKX_API_KEY / OKX_API_SECRET / OKX_API_PASSPHRASE

Запуск из папки с latency_test.py и конфигами:
    pip install -r requirements.txt
    python socket_test.py
"""

import time
import statistics
from websocket import create_connection

import latency_test as L   # переиспользуем загрузку .env, конфиги и логин OKX

N = 5           # число повторов
SPACING = 0.4    # пауза между подключениями, сек (лимит OKX ~3 conn/сек на IP)


def measure_socket(url, n=N):
    """Только открытие сокета: create_connection() -> close()."""
    times = []
    for i in range(n):
        if i:
            time.sleep(SPACING)
        t = time.perf_counter()
        ws = create_connection(url, timeout=10)
        times.append((time.perf_counter() - t) * 1000.0)
        ws.close()
    return times


def measure_okx_login(cfg, n=N):
    """Логин на уже открытом сокете: таймер только вокруг _login()."""
    times = []
    for i in range(n):
        if i:
            time.sleep(SPACING)
        c = L.OkxWs(cfg)
        c.ws = create_connection(cfg["ws_url"], timeout=10)
        t = time.perf_counter()
        c._login()
        times.append((time.perf_counter() - t) * 1000.0)
        c.close()
    return times


def stat(times):
    return min(times), statistics.median(times), max(times)


def row(label, times):
    mn, md, mx = stat(times)
    print(f"{label:<18}{mn:>12.2f}{md:>12.2f}{mx:>12.2f}")


def main():
    b = L.load_config(L.CONFIG_BINANCE)
    o = L.load_config(L.CONFIG_OKX)

    okx_key, okx_secret, okx_pass = L.okx_credentials()
    okx_shared = {k: o[k] for k in ("base_url", "ws_url", "simulated", "timeout_sec")
                  if k in o}
    okx_shared.update({"api_key": okx_key, "api_secret": okx_secret,
                       "passphrase": okx_pass})

    endpoints = [
        ("Binance Futures", b["futures"]["ws_url"]),
        ("Binance Spot",    b["spot"]["ws_url"]),
        ("OKX Futures",     o["ws_url"]),
        ("OKX Spot",        o["ws_url"]),
    ]

    print(f"Открытие WebSocket-сокета, мс (по {N} повторам)")
    print("─" * 54)
    print(f"{'':<18}{'min':>12}{'медиана':>12}{'max':>12}")
    print("─" * 54)
    for label, url in endpoints:
        try:
            row(label, measure_socket(url))
        except Exception as e:
            print(f"{label:<18}  ✗ {e}")
    print("─" * 54)

    print(f"\nЛогин OKX на открытом сокете, мс (по {N} повторам)")
    print("─" * 54)
    for market in ("futures", "spot"):
        cfg = {**okx_shared, **o[market]}
        try:
            row("OKX " + market, measure_okx_login(cfg))
        except Exception as e:
            print(f"{'OKX ' + market:<18}  ✗ {e}")
    print("─" * 54)
    print("Примечание: OKX Futures и OKX Spot - один и тот же сокет")
    print("(ws.okx.com), их числа совпадают. У Binance логина нет.")
    print("Первый повтор обычно дороже остальных - в него входит DNS.")


if __name__ == "__main__":
    main()
