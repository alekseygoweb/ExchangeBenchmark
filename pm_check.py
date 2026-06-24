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

print("\n" + "=" * 70)
if is_pm:
    print("ВЫВОД: счёт Portfolio Margin (Единый счёт).")
    print("Условные ордера идут через  POST /papi/v1/um/conditional/order")
    print("на  https://papi.binance.com  — обычный /fapi/v1/order их отклоняет (-4120).")
else:
    print("ВЫВОД: papi вернул не 200 — вероятно НЕ Portfolio Margin")
    print("(или у ключа нет прав на PM-эндпоинты). Тогда условные ордера —")
    print("через Futures Algo API. Покажи вывод выше для уточнения.")
print("=" * 70)
