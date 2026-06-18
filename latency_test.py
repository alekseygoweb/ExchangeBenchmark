#!/usr/bin/env python3
"""
Замер задержки цикла "разместить лимитный ордер -> отменить ордер"
на Binance и OKX, по фьючерсам и по споту.

ВНИМАНИЕ: скрипт работает на БОЕВЫХ счетах (PROD, РЕАЛЬНЫЕ ДЕНЬГИ).
Размещаются настоящие лимитные ордера и тут же отменяются. Ордера должны
стоять далеко от рынка (non-marketable), чтобы не исполниться, но риск
несёт пользователь. Перед запуском проверьте цены/объёмы в конфигах.

Ключи API НЕ хранятся в конфигах и НЕ коммитятся в git. Они берутся из
переменных окружения (файл .env, см. .env.example):
    BINANCE_API_KEY / BINANCE_API_SECRET
    OKX_API_KEY / OKX_API_SECRET / OKX_API_PASSPHRASE

Для каждого рынка замер делается двумя транспортами:
    API - обычные REST-запросы
    WS  - WebSocket (соединение и логин выносятся ЗА таймер,
          поэтому замеряется чистая отправка без хендшейка)

По каждому транспорту засекаются три величины (в мс):
    Размещение  - от отправки ордера до подтверждения размещения
    Отмена      - от отправки запроса на отмену до подтверждения отмены
    Итого       - сумма

Зависимости:
    pip install -r requirements.txt
    (requests, websocket-client, python-dotenv)
"""

import os
import sys
import json
import time
import hmac
import uuid
import base64
import hashlib
from datetime import datetime, timezone
from urllib.parse import urlencode

import requests

try:
    from websocket import create_connection
    WS_AVAILABLE = True
except ImportError:
    WS_AVAILABLE = False


# =========================================================================== #
#                          Загрузка ключей из .env                            #
# =========================================================================== #
def load_env_file(path=".env"):
    """
    Подгружаем переменные окружения из файла .env.

    Если установлен python-dotenv - используем его. Если нет - применяем
    минимальный встроенный парсер (KEY=VALUE построчно), чтобы скрипт
    работал и без дополнительной зависимости. Уже заданные переменные
    окружения не перезаписываются (приоритет у реального окружения).
    """
    try:
        from dotenv import load_dotenv
        load_dotenv(path)
        return
    except ImportError:
        pass

    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            if line.lower().startswith("export "):
                line = line[len("export "):]
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val


load_env_file()

# Пути к конфигам можно переопределить через окружение/.env.
CONFIG_BINANCE = os.environ.get("CONFIG_BINANCE", "config_binance_PROD.json")
CONFIG_OKX = os.environ.get("CONFIG_OKX", "config_okx_PROD.json")


def _require_env(name):
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(
            f"не задана переменная окружения {name} "
            f"(заполните .env по образцу .env.example)")
    return val


def binance_credentials(market):
    """
    Ключи Binance: сначала смотрим переопределение под конкретный рынок
    (BINANCE_FUTURES_API_KEY / BINANCE_SPOT_API_KEY), затем общий ключ
    (BINANCE_API_KEY). Так можно использовать как один ключ на оба рынка,
    так и раздельные.
    """
    prefix = "BINANCE_" + market.upper() + "_"
    key = os.environ.get(prefix + "API_KEY") or os.environ.get("BINANCE_API_KEY")
    secret = os.environ.get(prefix + "API_SECRET") or os.environ.get("BINANCE_API_SECRET")
    if not key or not secret:
        raise RuntimeError(
            f"нет ключей Binance для рынка '{market}': задайте "
            f"BINANCE_API_KEY/BINANCE_API_SECRET (или {prefix}API_KEY/"
            f"{prefix}API_SECRET) в .env")
    return key, secret


def okx_credentials():
    return (
        _require_env("OKX_API_KEY"),
        _require_env("OKX_API_SECRET"),
        _require_env("OKX_API_PASSPHRASE"),
    )


def load_config(path):
    if not os.path.exists(path):
        raise RuntimeError(f"конфиг не найден: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def now_ms():
    return int(time.time() * 1000)


def gen_cl_id():
    # Клиентский идентификатор ордера: буквы+цифры, до 32 символов.
    # Позволяет отменять ордер по своему ID, не дожидаясь биржевого orderId.
    return "lt" + uuid.uuid4().hex[:20]


# =========================================================================== #
#                              REST (API)                                     #
# =========================================================================== #
class BinanceRest:
    def __init__(self, cfg, market):
        self.cfg = cfg
        self.base = cfg["base_url"].rstrip("/")
        self.key = cfg["api_key"]
        self.secret = cfg["api_secret"].encode()
        self.timeout = cfg.get("timeout_sec", 10)
        self.path = "/fapi/v1/order" if market == "futures" else "/api/v3/order"
        self.session = requests.Session()
        self.session.headers.update({"X-MBX-APIKEY": self.key})

    def _sign(self, params):
        query = urlencode(params)
        sig = hmac.new(self.secret, query.encode(), hashlib.sha256).hexdigest()
        return query + "&signature=" + sig

    def _signed(self, method, params):
        params = dict(params)
        params["timestamp"] = now_ms()
        params["recvWindow"] = self.cfg.get("recv_window", 5000)
        url = f"{self.base}{self.path}?{self._sign(params)}"
        r = self.session.request(method, url, timeout=self.timeout)
        data = r.json()
        if r.status_code != 200:
            raise RuntimeError(f"Binance API {r.status_code}: {data}")
        return data

    def place_order(self):
        cl = gen_cl_id()
        data = self._signed("POST", {
            "symbol": self.cfg["symbol"],
            "side": self.cfg.get("side", "BUY"),
            "type": "LIMIT",
            "timeInForce": self.cfg.get("time_in_force", "GTC"),
            "quantity": self.cfg["quantity"],
            "price": self.cfg["price"],
            "newClientOrderId": cl,
        })
        if data.get("status") not in ("NEW", "PARTIALLY_FILLED", "FILLED"):
            raise RuntimeError(f"ордер не размещён: {data}")
        return cl

    def cancel_order(self, cl):
        data = self._signed("DELETE", {"symbol": self.cfg["symbol"], "origClientOrderId": cl})
        if data.get("status") != "CANCELED":
            raise RuntimeError(f"ордер не отменён: {data}")


class OkxRest:
    def __init__(self, cfg):
        self.cfg = cfg
        self.base = cfg["base_url"].rstrip("/")
        self.key = cfg["api_key"]
        self.secret = cfg["api_secret"].encode()
        self.passphrase = cfg["passphrase"]
        self.timeout = cfg.get("timeout_sec", 10)
        self.session = requests.Session()

    def _ts(self):
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    def _sign(self, ts, method, path, body):
        msg = (ts + method + path + body).encode()
        return base64.b64encode(hmac.new(self.secret, msg, hashlib.sha256).digest()).decode()

    def _request(self, method, path, body):
        ts = self._ts()
        body_str = json.dumps(body) if body else ""
        headers = {
            "OK-ACCESS-KEY": self.key,
            "OK-ACCESS-SIGN": self._sign(ts, method, path, body_str),
            "OK-ACCESS-TIMESTAMP": ts,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
        }
        if self.cfg.get("simulated", False):
            headers["x-simulated-trading"] = "1"
        r = self.session.request(method, self.base + path, headers=headers,
                                 data=body_str, timeout=self.timeout)
        data = r.json()
        if data.get("code") != "0":
            raise RuntimeError(f"OKX API: {data}")
        return data

    def place_order(self):
        cl = gen_cl_id()
        item = self._request("POST", "/api/v5/trade/order", {
            "instId": self.cfg["inst_id"], "tdMode": self.cfg["td_mode"],
            "side": self.cfg.get("side", "buy"), "ordType": "limit",
            "px": self.cfg["price"], "sz": self.cfg["size"], "clOrdId": cl,
        })["data"][0]
        if item.get("sCode") != "0":
            raise RuntimeError(f"ордер не размещён: {item}")
        return cl

    def cancel_order(self, cl):
        item = self._request("POST", "/api/v5/trade/cancel-order",
                             {"instId": self.cfg["inst_id"], "clOrdId": cl})["data"][0]
        if item.get("sCode") != "0":
            raise RuntimeError(f"ордер не отменён: {item}")


# =========================================================================== #
#                              WebSocket (WS)                                 #
# =========================================================================== #
class BinanceWs:
    def __init__(self, cfg, market):
        self.cfg = cfg
        self.url = cfg["ws_url"]
        # WS может жить в другой системе, чем REST - тогда нужен отдельный ключ.
        # Для PROD это обычно тот же ключ, что и у REST.
        self.key = cfg.get("ws_api_key", cfg["api_key"])
        self.secret = cfg.get("ws_api_secret", cfg["api_secret"]).encode()
        self.timeout = cfg.get("timeout_sec", 10)
        self.ws = None

    def connect(self):  # вне таймера
        self.ws = create_connection(self.url, timeout=self.timeout)

    def close(self):
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass

    def _sign(self, params):
        query = urlencode(sorted(params.items()))
        return hmac.new(self.secret, query.encode(), hashlib.sha256).hexdigest()

    def _call(self, method, params):
        params = dict(params)
        params["apiKey"] = self.key
        params["timestamp"] = now_ms()
        params["signature"] = self._sign(params)
        req_id = uuid.uuid4().hex
        self.ws.send(json.dumps({"id": req_id, "method": method, "params": params}))
        while True:
            msg = json.loads(self.ws.recv())
            if msg.get("id") != req_id:
                continue
            if msg.get("status") != 200:
                raise RuntimeError(f"Binance WS: {msg.get('error')}")
            return msg["result"]

    def place_order(self):
        cl = gen_cl_id()
        res = self._call("order.place", {
            "symbol": self.cfg["symbol"],
            "side": self.cfg.get("side", "BUY"),
            "type": "LIMIT",
            "timeInForce": self.cfg.get("time_in_force", "GTC"),
            "quantity": self.cfg["quantity"],
            "price": self.cfg["price"],
            "newClientOrderId": cl,
        })
        if res.get("status") not in ("NEW", "PARTIALLY_FILLED", "FILLED"):
            raise RuntimeError(f"ордер не размещён: {res}")
        return cl

    def cancel_order(self, cl):
        res = self._call("order.cancel", {"symbol": self.cfg["symbol"], "origClientOrderId": cl})
        if res.get("status") != "CANCELED":
            raise RuntimeError(f"ордер не отменён: {res}")


class OkxWs:
    def __init__(self, cfg):
        self.cfg = cfg
        self.url = cfg["ws_url"]
        self.key = cfg["api_key"]
        self.secret = cfg["api_secret"].encode()
        self.passphrase = cfg["passphrase"]
        self.timeout = cfg.get("timeout_sec", 10)
        self.ws = None
        self.inst_id_code = None

    def connect(self):  # вне таймера: соединение + логин + код инструмента
        self.inst_id_code = self._fetch_inst_id_code()
        self.ws = create_connection(self.url, timeout=self.timeout)
        self._login()

    def _fetch_inst_id_code(self):
        # OKX требует instIdCode в WS-ордерах; мапим instId -> код по REST.
        inst_type = "SWAP" if self.cfg["inst_id"].endswith("-SWAP") else "SPOT"
        headers = {}
        if self.cfg.get("simulated", False):
            headers["x-simulated-trading"] = "1"
        r = requests.get(self.cfg["base_url"].rstrip("/") + "/api/v5/public/instruments",
                         params={"instType": inst_type, "instId": self.cfg["inst_id"]},
                         headers=headers, timeout=self.timeout)
        data = r.json()
        if data.get("code") != "0" or not data.get("data"):
            raise RuntimeError(f"не получен instIdCode: {data}")
        code = data["data"][0].get("instIdCode")
        if code in (None, ""):
            raise RuntimeError(f"instIdCode пуст в ответе: {data['data'][0]}")
        return code

    def close(self):
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass

    def _login(self):
        ts = str(int(time.time()))
        sign = base64.b64encode(
            hmac.new(self.secret, (ts + "GET" + "/users/self/verify").encode(),
                     hashlib.sha256).digest()).decode()
        self.ws.send(json.dumps({"op": "login", "args": [{
            "apiKey": self.key, "passphrase": self.passphrase,
            "timestamp": ts, "sign": sign,
        }]}))
        while True:
            raw = self.ws.recv()
            if not raw:
                continue
            m = json.loads(raw)
            if m.get("event") == "login" and m.get("code") == "0":
                return
            if m.get("event") == "error":
                raise RuntimeError(f"OKX WS login: {m}")

    def _call(self, op, arg):
        req_id = uuid.uuid4().hex[:16]
        self.ws.send(json.dumps({"id": req_id, "op": op, "args": [arg]}))
        while True:
            raw = self.ws.recv()
            if not raw:
                continue
            m = json.loads(raw)
            if m.get("id") != req_id:
                continue
            if m.get("code") != "0":
                raise RuntimeError(f"OKX WS {op}: {m}")
            return m["data"][0]

    def place_order(self):
        cl = gen_cl_id()
        item = self._call("order", {
            "instId": self.cfg["inst_id"], "instIdCode": self.inst_id_code,
            "tdMode": self.cfg["td_mode"],
            "side": self.cfg.get("side", "buy"), "ordType": "limit",
            "px": self.cfg["price"], "sz": self.cfg["size"], "clOrdId": cl,
        })
        if item.get("sCode") != "0":
            raise RuntimeError(f"ордер не размещён: {item}")
        return cl

    def cancel_order(self, cl):
        item = self._call("cancel-order", {
            "instId": self.cfg["inst_id"], "instIdCode": self.inst_id_code,
            "clOrdId": cl,
        })
        if item.get("sCode") != "0":
            raise RuntimeError(f"ордер не отменён: {item}")


# =========================================================================== #
#                                 Замер                                       #
# =========================================================================== #
REPEATS = int(os.environ.get("LATENCY_REPEATS", "5"))  # циклов "отправка+отмена"


def _ms(a, b):
    return (b - a) * 1000.0


def measure(place_fn, cancel_fn, repeats=REPEATS):
    """
    1) Первый ордер: меряем только размещение (для REST оно "холодное" -
       включает установку соединения). Его отмену НЕ учитываем - чистим.
    2) На прогретом соединении повторяем repeats раз "отправка+отмена",
       усредняем размещения и отмены по повторам.
    Итого повт. = среднее размещение + средняя отмена.
    """
    t0 = time.perf_counter()
    order_id = place_fn()                       # подтверждение размещения
    first_place = _ms(t0, time.perf_counter())
    cancel_fn(order_id)                         # отмена первого - не считаем

    places, cancels = [], []
    for _ in range(repeats):
        a = time.perf_counter()
        oid = place_fn()
        b = time.perf_counter()
        cancel_fn(oid)
        c = time.perf_counter()
        places.append(_ms(a, b))
        cancels.append(_ms(b, c))

    avg_place = sum(places) / len(places)
    avg_cancel = sum(cancels) / len(cancels)
    return {
        "first": first_place,
        "place": avg_place,
        "cancel": avg_cancel,
        "total": avg_place + avg_cancel,
    }


def measure_ws(client):
    if not WS_AVAILABLE:
        raise RuntimeError("нет websocket-client (pip install websocket-client)")
    client.connect()                  # соединение/логин - ВНЕ таймера
    try:
        return measure(client.place_order, client.cancel_order)
    finally:
        client.close()


def run_binance(market, transport):
    cfg = dict(load_config(CONFIG_BINANCE)[market])
    cfg["api_key"], cfg["api_secret"] = binance_credentials(market)
    if transport == "API":
        c = BinanceRest(cfg, market)
        return measure(c.place_order, c.cancel_order)
    return measure_ws(BinanceWs(cfg, market))


def run_okx(market, transport):
    full = load_config(CONFIG_OKX)
    shared = {k: full[k] for k in ("base_url", "ws_url", "simulated", "timeout_sec")
              if k in full}
    cfg = {**shared, **full[market]}
    cfg["api_key"], cfg["api_secret"], cfg["passphrase"] = okx_credentials()
    if transport == "API":
        c = OkxRest(cfg)
        return measure(c.place_order, c.cancel_order)
    return measure_ws(OkxWs(cfg))


MARKETS = [
    ("Binance Futures", run_binance, "futures"),
    ("Binance Spot",    run_binance, "spot"),
    ("OKX Futures",     run_okx,     "futures"),
    ("OKX Spot",        run_okx,     "spot"),
]


# =========================================================================== #
#                                 Таблица                                     #
# =========================================================================== #
def print_table(results):
    W_LABEL, W_NUM = 18, 15
    line = "─" * (W_LABEL + W_NUM * 4)
    h1 = ("Первый", "Повторный", "Отмена", "Итого")
    h2 = ("ордер", "ордер (ср.)", "ордера (ср.)", "повт. (ср.)")

    print()
    print("  Задержка лимитного ордера (БОЕВОЙ счёт), мс")
    print("  «Повторный» — среднее по {} циклам на прогретом соединении".format(REPEATS))
    print(line)
    print(f"{'':<{W_LABEL}}" + "".join(f"{x:>{W_NUM}}" for x in h1))
    print(f"{'':<{W_LABEL}}" + "".join(f"{x:>{W_NUM}}" for x in h2))
    print(line)

    for label, _, _ in MARKETS:
        print(label)
        row = results[label]
        for transport, disp in (("API", "REST API"), ("WS", "WS API")):
            v = row[transport]
            tag = f"  {disp}"
            if isinstance(v, dict):
                cells = (v["first"], v["place"], v["cancel"], v["total"])
                print(f"{tag:<{W_LABEL}}" + "".join(f"{x:>{W_NUM}.2f}" for x in cells))
            else:
                msg = str(v)
                msg = msg if len(msg) <= 44 else msg[:44] + "…"
                print(f"{tag:<{W_LABEL}}  ✗ {msg}")
    print(line)


def confirm_live(skip):
    """Подтверждение перед торговлей реальными деньгами на PROD."""
    if skip:
        return
    bar = "!" * 70
    print("\n" + bar)
    print("ВНИМАНИЕ: будут размещены РЕАЛЬНЫЕ ордера на БОЕВЫХ счетах (PROD).")
    print("Ордера лимитные и стоят далеко от рынка, сразу отменяются, но это")
    print("РЕАЛЬНЫЕ ДЕНЬГИ. Убедитесь, что цены/объёмы в конфигах безопасны.")
    print(bar)
    try:
        ans = input("Введите 'yes' для продолжения: ").strip().lower()
    except EOFError:
        ans = ""
    if ans not in ("yes", "y", "да"):
        print("Отменено.")
        sys.exit(1)


def main():
    skip_confirm = ("--yes" in sys.argv or "-y" in sys.argv
                    or os.environ.get("LATENCY_CONFIRM", "").lower() in ("1", "yes", "true"))
    confirm_live(skip_confirm)

    print("Замер задержки (БОЕВЫЕ счета). Прогон может занять несколько секунд...")
    results = {}
    for label, fn, market in MARKETS:
        row = {}
        for transport in ("API", "WS"):
            try:
                row[transport] = fn(market, transport)
            except Exception as e:
                row[transport] = e
        results[label] = row
    print_table(results)


if __name__ == "__main__":
    main()
