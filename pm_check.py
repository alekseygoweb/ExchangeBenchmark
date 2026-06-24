#!/usr/bin/env python3
"""Диагностика: определить, является ли Binance-счёт Portfolio Margin (Единый
счёт). От этого зависит, через какой эндпоинт идут УСЛОВНЫЕ (trigger) ордера.

Запуск:
    python3 pm_check.py

Только GET-запросы (баланс/тип счёта), НИЧЕГО не торгует. Ключи берутся из
окружения или из .env (BINANCE_API_KEY / BINANCE_API_SECRET, либо
BINANCE_FUTURES_API_KEY / BINANCE_FUTURES_API_SECRET).
"""
import hashlib
import hmac
import os
import sys
import time
import urllib.parse

import requests

# ── .env (минимальный парсер, реальное окружение имеет приоритет) ──────────────
if os.path.exists(".env"):
    for line in open(".env"):
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())

key = os.environ.get("BINANCE_FUTURES_API_KEY") or os.environ.get("BINANCE_API_KEY")
secret = (os.environ.get("BINANCE_FUTURES_API_SECRET")
          or os.environ.get("BINANCE_API_SECRET") or "").encode()
if not key or not secret:
    print("✗ Нет ключей: задайте BINANCE_API_KEY/BINANCE_API_SECRET "
          "(в окружении или .env)")
    sys.exit(1)


def signed_get(base, path):
    params = {"timestamp": int(time.time() * 1000), "recvWindow": 5000}
    q = urllib.parse.urlencode(params)
    sig = hmac.new(secret, q.encode(), hashlib.sha256).hexdigest()
    url = base + path + "?" + q + "&signature=" + sig
    try:
        r = requests.get(url, headers={"X-MBX-APIKEY": key}, timeout=10)
        return r.status_code, r.text
    except Exception as e:  # noqa: BLE001
        return None, f"EXC {e}"


print("=" * 70)
print("Проверка типа счёта Binance")
print("=" * 70)

# 1) Portfolio Margin balance (papi.binance.com) — 200 ⇒ PM включён
print("\n[1] Portfolio Margin: GET https://papi.binance.com/papi/v1/balance")
sc, body = signed_get("https://papi.binance.com", "/papi/v1/balance")
print(f"    HTTP {sc}")
print("    " + body[:400].replace("\n", " "))
is_pm = (sc == 200)

# 2) Обычный фьючерсный аккаунт (fapi) — для контраста, что ключ рабочий
print("\n[2] Обычные фьючерсы: GET https://fapi.binance.com/fapi/v2/account")
sc2, body2 = signed_get("https://fapi.binance.com", "/fapi/v2/account")
print(f"    HTTP {sc2}")
if sc2 == 200:
    print("    (ответ получен — ключ валиден для fapi)")
else:
    print("    " + body2[:400].replace("\n", " "))

# 3) Разрешения ключа (sapi на api.binance.com) — флаг PM снимает
#    двусмысленность -2015 (нет PM-прав vs счёт не PM).
print("\n[3] Разрешения ключа: GET https://api.binance.com/sapi/v1/account/apiRestrictions")
sc3, body3 = signed_get("https://api.binance.com", "/sapi/v1/account/apiRestrictions")
print(f"    HTTP {sc3}")
print("    " + body3[:500].replace("\n", " "))
pm_flag = None
if sc3 == 200 and "enablePortfolioMarginTrading" in body3:
    import json as _json
    try:
        pm_flag = bool(_json.loads(body3).get("enablePortfolioMarginTrading"))
    except Exception:  # noqa: BLE001
        pm_flag = None

print("\n" + "=" * 70)
if is_pm:
    print("ВЫВОД: счёт Portfolio Margin (papi доступен).")
    print("Условные ордера идут через  POST /papi/v1/um/conditional/order")
    print("на  https://papi.binance.com  — обычный /fapi/v1/order их отклоняет (-4120).")
elif pm_flag is True:
    print("ВЫВОД: у ключа ВКЛЮЧЁН Portfolio Margin Trading, но papi дал -2015.")
    print("Вероятно нужно домкнуть права/привязать ключ к PM — но счёт PM. Реализуем papi.")
elif pm_flag is False:
    print("ВЫВОД: у ключа Portfolio Margin Trading ВЫКЛЮЧЕН.")
    print("Если счёт всё же PM — включи это право у ключа и перезапусти.")
    print("Если счёт классический — условные через Futures Algo API (простого стопа там нет).")
else:
    print("ВЫВОД: однозначно по API не вышло (sapi/papi не дали PM-флаг).")
    print("Проверь режим счёта в интерфейсе Binance (Wallet → Portfolio Margin).")
print("=" * 70)
