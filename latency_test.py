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
from decimal import Decimal, ROUND_DOWN, ROUND_UP
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
CONFIG_MEXC = os.environ.get("CONFIG_MEXC", "config_mexc_PROD.json")
# Отдельный конфиг для условного (trigger) бенчмарка Binance — включается флагом
# --conditional / CONDITIONAL=1. Держим его отдельным файлом, чтобы обычный прогон
# по умолчанию оставался безопасным лимитным, а «заряженный» конфиг был явным opt-in.
CONFIG_BINANCE_CONDITIONAL = os.environ.get(
    "CONFIG_BINANCE_CONDITIONAL", "config_binance_PROD_conditional.json")
CONFIG_OKX_CONDITIONAL = os.environ.get(
    "CONFIG_OKX_CONDITIONAL", "config_okx_PROD_conditional.json")
CONFIG_MEXC_CONDITIONAL = os.environ.get(
    "CONFIG_MEXC_CONDITIONAL", "config_mexc_PROD_conditional.json")


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


def mexc_credentials():
    return _require_env("MEXC_API_KEY"), _require_env("MEXC_API_SECRET")


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


# Условные (trigger) типы Binance: лимитные требуют price+timeInForce,
# рыночные (*_MARKET) — нет. stopPrice обязателен для всех условных.
LIMIT_CONDITIONAL_TYPES = {"STOP", "TAKE_PROFIT", "STOP_LOSS_LIMIT", "TAKE_PROFIT_LIMIT"}
MARKET_CONDITIONAL_TYPES = {"STOP_MARKET", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET"}


def is_conditional(cfg):
    return str(cfg.get("type", "LIMIT")).upper() != "LIMIT"


def binance_order_params(cfg, is_futures, cl):
    """Параметры ордера Binance: обычный LIMIT либо условный (trigger).

    Тип берётся из cfg["type"] (по умолчанию LIMIT). Один и тот же конструктор
    используют REST и WS, чтобы оба транспорта слали идентичный ордер.
    """
    otype = str(cfg.get("type", "LIMIT")).upper()
    allowed = {"LIMIT"} | LIMIT_CONDITIONAL_TYPES | MARKET_CONDITIONAL_TYPES
    if otype not in allowed:                     # fail-fast на опечатке в конфиге
        raise RuntimeError(
            f"неизвестный тип ордера '{otype}'; допустимо: {sorted(allowed)}")
    params = {
        "symbol": cfg["symbol"],
        "side": cfg.get("side", "BUY"),
        "type": otype,
        "quantity": cfg["quantity"],
        "newClientOrderId": cl,
    }
    is_market_cond = otype in MARKET_CONDITIONAL_TYPES
    if not is_market_cond:                      # LIMIT и лимитные условные
        params["price"] = cfg["price"]
        params["timeInForce"] = cfg.get("time_in_force", "GTC")
    if is_market_cond or otype in LIMIT_CONDITIONAL_TYPES:
        params["stopPrice"] = cfg["stop_price"]
        if is_futures and cfg.get("working_type"):
            params["workingType"] = cfg["working_type"]
    if is_futures and cfg.get("position_side"):     # hedge-режим: LONG/SHORT
        params["positionSide"] = cfg["position_side"]
    return params


def binance_algo_conditional_params(cfg, cl):
    """Параметры УСЛОВНОГО ордера для нового Algo-эндпоинта Binance Futures
    (POST /fapi/v1/algoOrder). С 2025-12-09 условные типы (STOP_MARKET, STOP,
    TAKE_PROFIT_MARKET, TAKE_PROFIT, TRAILING_STOP_MARKET) на /fapi/v1/order
    отклоняются (-4120) и должны идти сюда.

    Имена полей в теле запроса для ЛИНЕЙНОГО бессрочного НЕ-PM ордера — смесь
    (сверено с ccxt create_order_request): type как у обычного ордера, НО триггер
    идёт как triggerPrice (не stopPrice), а клиентский id — как clientAlgoId
    (не newClientOrderId). Плюс обязательный algoType=CONDITIONAL. Лимитные
    условные (STOP/TAKE_PROFIT) добавляют price+timeInForce, рыночные — нет."""
    otype = str(cfg.get("type", "STOP_MARKET")).upper()
    if otype not in (LIMIT_CONDITIONAL_TYPES | MARKET_CONDITIONAL_TYPES):
        raise RuntimeError(f"algo conditional: тип '{otype}' не условный")
    params = {
        "algoType": "CONDITIONAL",
        "symbol": cfg["symbol"],
        "side": cfg.get("side", "BUY"),
        "type": otype,
        "quantity": cfg["quantity"],
        "triggerPrice": cfg["stop_price"],
        "clientAlgoId": cl,
        "newOrderRespType": "RESULT",              # иначе дефолт ACK без algoStatus
    }
    if otype in LIMIT_CONDITIONAL_TYPES:            # лимитные условные: цена + TIF
        params["price"] = cfg["price"]
        params["timeInForce"] = cfg.get("time_in_force", "GTC")
    if cfg.get("working_type"):
        params["workingType"] = cfg["working_type"]
    if cfg.get("position_side"):                    # hedge-режим: LONG/SHORT
        params["positionSide"] = cfg["position_side"]
    return params


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
        self.market = market
        self.is_futures = market == "futures"
        self.symbol = cfg["symbol"]
        self.path = "/fapi/v1/order" if self.is_futures else "/api/v3/order"
        self.open_path = "/fapi/v1/openOrders" if self.is_futures else "/api/v3/openOrders"
        # С 2025-12-09 условные (trigger) ордера USDⓈ-M futures идут через
        # отдельный Algo-эндпоинт /fapi/v1/algoOrder (тот же хост fapi, другой
        # путь и имена полей). Обычный /fapi/v1/order их отклоняет с -4120.
        self.algo = self.is_futures and is_conditional(cfg)
        self.session = requests.Session()
        self.session.headers.update({"X-MBX-APIKEY": self.key})

    def _sign(self, params):
        query = urlencode(params)
        sig = hmac.new(self.secret, query.encode(), hashlib.sha256).hexdigest()
        return query + "&signature=" + sig

    def _signed(self, method, params):
        return self._signed_path(method, self.path, params)

    def _signed_path(self, method, path, params):
        params = dict(params)
        params["timestamp"] = now_ms()
        params["recvWindow"] = self.cfg.get("recv_window", 5000)
        url = f"{self.base}{path}?{self._sign(params)}"
        r = self.session.request(method, url, timeout=self.timeout)
        try:                                        # пустой/не-JSON ответ → понятная ошибка
            data = r.json()
        except ValueError:
            raise RuntimeError(
                f"Binance API {r.status_code}: не-JSON ответ {r.text[:200]!r}")
        if r.status_code != 200:
            raise RuntimeError(f"Binance API {r.status_code}: {data}")
        return data

    def place_order(self):
        cl = gen_cl_id()
        if self.algo:                               # условный → новый Algo-эндпоинт
            data = self._signed_path("POST", "/fapi/v1/algoOrder",
                                     binance_algo_conditional_params(self.cfg, cl))
            # Успех: есть серверный algoId ИЛИ algoStatus валиден (на случай
            # урезанного ACK-ответа без algoStatus). Отмена идёт по нашему cl.
            ok = (data.get("algoId") is not None
                  or data.get("algoStatus") in ("NEW", "WORKING", "TRIGGERED"))
            if not ok:
                raise RuntimeError(f"условный ордер не размещён: {data}")
            return cl
        data = self._signed("POST", binance_order_params(self.cfg, self.is_futures, cl))
        if data.get("status") not in ("NEW", "PARTIALLY_FILLED", "FILLED"):
            raise RuntimeError(f"ордер не размещён: {data}")
        return cl

    def dual_side_position(self):
        """True, если фьючерсный счёт в режиме хеджирования (dual-side)."""
        data = self._signed_path("GET", "/fapi/v1/positionSide/dual", {})
        return bool(data.get("dualSidePosition", False))

    def available_usdt(self):
        """Свободный баланс USDT (для balance-guard перед условным ордером)."""
        if self.is_futures:
            data = self._signed_path("GET", "/fapi/v2/balance", {})
            for b in data:
                if b.get("asset") == "USDT":
                    return float(b.get("availableBalance", 0) or 0)
            return 0.0
        data = self._signed_path("GET", "/api/v3/account", {})
        for b in data.get("balances", []):
            if b.get("asset") == "USDT":
                return float(b.get("free", 0) or 0)
        return 0.0

    def cancel_order(self, cl):
        if self.algo:                               # условный → отмена на Algo-эндпоинте
            # Cancel Algo Order принимает только algoId/clientAlgoId — symbol
            # НЕ входит в параметры (лишний параметр → риск -1104).
            data = self._signed_path(
                "DELETE", "/fapi/v1/algoOrder", {"clientAlgoId": cl})
            # Ответ отмены — {algoId, clientAlgoId, code:"200", msg:"success"}
            # (без algoStatus). Успех: code 200 / msg success / algoStatus CANCELED.
            code = str(data.get("code")) if data.get("code") is not None else None
            st = data.get("algoStatus")
            ok = (code in (None, "200")) and (st in (None, "CANCELED"))
            if not ok and data.get("msg") != "success":
                raise RuntimeError(f"условный ордер не отменён: {data}")
            return
        data = self._signed("DELETE", {"symbol": self.cfg["symbol"], "origClientOrderId": cl})
        if data.get("status") != "CANCELED":
            raise RuntimeError(f"ордер не отменён: {data}")

    def list_open(self):
        if self.algo:
            return self._signed_path("GET", "/fapi/v1/openAlgoOrders", {"symbol": self.symbol})
        return self._signed_path("GET", self.open_path, {"symbol": self.symbol})

    def position_amt(self):
        if not self.is_futures:
            return 0.0
        data = self._signed_path("GET", "/fapi/v2/positionRisk", {"symbol": self.symbol})
        return sum(float(p.get("positionAmt", 0) or 0) for p in data)

    def reconcile(self):
        # Снимаем ТОЛЬКО свои ордера (id с префиксом "lt"), чужие по символу не
        # трогаем. У algo-ордеров клиентский id лежит в clientAlgoId. Возвращаем
        # (снято, осталось, позиция).
        id_field = "clientAlgoId" if self.algo else "clientOrderId"
        ours = [o for o in self.list_open()
                if str(o.get(id_field, "")).startswith("lt")]
        for o in ours:
            try:
                if self.algo:
                    self._signed_path("DELETE", "/fapi/v1/algoOrder",
                                      {"clientAlgoId": o[id_field]})
                else:
                    self._signed_path("DELETE", self.path,
                                      {"symbol": self.symbol,
                                       "origClientOrderId": o[id_field]})
            except Exception:
                pass
        left = [o for o in self.list_open()
                if str(o.get(id_field, "")).startswith("lt")]
        return len(ours), len(left), self.position_amt()


# Условные (algo) типы OKX: размещаются через /api/v5/trade/order-algo, а не
# /api/v5/trade/order. Для benchmark используем "trigger" (стоп с triggerPx).
OKX_ALGO_TYPES = {"trigger", "conditional", "oco", "move_order_stop"}


def okx_is_conditional(cfg):
    return str(cfg.get("ord_type", "limit")).lower() in OKX_ALGO_TYPES


def okx_algo_params(cfg, cl):
    """Тело algo-ордера OKX (POST /api/v5/trade/order-algo). Для trigger: triggerPx
    (цена срабатывания) + orderPx (-1 = рыночный ордер при срабатывании). Наш
    клиентский id идёт в algoClOrdId (по нему чистит reconcile)."""
    p = {
        "instId": cfg["inst_id"],
        "tdMode": cfg["td_mode"],
        "side": cfg.get("side", "buy"),
        "ordType": cfg.get("ord_type", "trigger"),
        "sz": cfg["size"],
        "triggerPx": cfg["trigger_px"],
        "orderPx": cfg.get("order_px", "-1"),
        "algoClOrdId": cl,
    }
    if cfg.get("trigger_px_type"):                  # last/index/mark
        p["triggerPxType"] = cfg["trigger_px_type"]
    if cfg.get("pos_side"):                          # long/short режим (hedge)
        p["posSide"] = cfg["pos_side"]
    return p


class OkxRest:
    def __init__(self, cfg):
        self.cfg = cfg
        self.algo = okx_is_conditional(cfg)
        self.base = cfg["base_url"].rstrip("/")
        self.key = cfg["api_key"]
        self.secret = cfg["api_secret"].encode()
        self.passphrase = cfg["passphrase"]
        self.timeout = cfg.get("timeout_sec", 10)
        self.inst = cfg["inst_id"]
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
        if self.algo:                               # условный → algo-эндпоинт
            item = self._request("POST", "/api/v5/trade/order-algo",
                                 okx_algo_params(self.cfg, cl))["data"][0]
            if item.get("sCode") != "0":
                raise RuntimeError(f"условный ордер не размещён: {item}")
            return item.get("algoId")               # отмена идёт по algoId
        body = {
            "instId": self.cfg["inst_id"], "tdMode": self.cfg["td_mode"],
            "side": self.cfg.get("side", "buy"), "ordType": "limit",
            "px": self.cfg["price"], "sz": self.cfg["size"], "clOrdId": cl,
        }
        if self.cfg.get("pos_side"):                # long/short-режим
            body["posSide"] = self.cfg["pos_side"]
        item = self._request("POST", "/api/v5/trade/order", body)["data"][0]
        if item.get("sCode") != "0":
            raise RuntimeError(f"ордер не размещён: {item}")
        return cl

    def cancel_order(self, oid):
        if self.algo:                               # условный → cancel-algos по algoId
            item = self._request("POST", "/api/v5/trade/cancel-algos",
                                 [{"algoId": oid, "instId": self.inst}])["data"][0]
            if item.get("sCode") != "0":
                raise RuntimeError(f"условный ордер не отменён: {item}")
            return
        item = self._request("POST", "/api/v5/trade/cancel-order",
                             {"instId": self.cfg["inst_id"], "clOrdId": oid})["data"][0]
        if item.get("sCode") != "0":
            raise RuntimeError(f"ордер не отменён: {item}")

    def available_usdt(self):
        """Свободный баланс USDT (для balance-guard перед условным ордером)."""
        data = self._request("GET", "/api/v5/account/balance?ccy=USDT", None)["data"]
        if not data:
            return 0.0
        for d in data[0].get("details", []):
            if d.get("ccy") == "USDT":
                return float(d.get("availBal") or d.get("cashBal") or 0)
        return float(data[0].get("totalEq") or 0)

    def list_open(self):
        if self.algo:
            ot = self.cfg.get("ord_type", "trigger")
            return self._request(
                "GET", f"/api/v5/trade/orders-algo-pending?ordType={ot}&instId={self.inst}",
                None)["data"]
        return self._request("GET", f"/api/v5/trade/orders-pending?instId={self.inst}",
                             None)["data"]

    def position_amt(self):
        if not self.inst.endswith("-SWAP"):
            return 0.0
        data = self._request("GET", f"/api/v5/account/positions?instId={self.inst}",
                             None)["data"]
        return sum(float(p.get("pos") or 0) for p in data)

    def reconcile(self):
        # Снимаем ТОЛЬКО свои ордера (id с префиксом "lt"). У algo-ордеров
        # клиентский id лежит в algoClOrdId, отмена — по algoId.
        id_field = "algoClOrdId" if self.algo else "clOrdId"
        ours = [o for o in self.list_open()
                if str(o.get(id_field, "")).startswith("lt")]
        for o in ours:
            try:
                if self.algo:
                    self._request("POST", "/api/v5/trade/cancel-algos",
                                  [{"algoId": o["algoId"], "instId": self.inst}])
                else:
                    self._request("POST", "/api/v5/trade/cancel-order",
                                  {"instId": self.inst, "ordId": o["ordId"]})
            except Exception:
                pass
        left = [o for o in self.list_open()
                if str(o.get(id_field, "")).startswith("lt")]
        return len(ours), len(left), self.position_amt()


# =========================================================================== #
#                              WebSocket (WS)                                 #
# =========================================================================== #
class BinanceWs:
    def __init__(self, cfg, market):
        self.cfg = cfg
        self.url = cfg["ws_url"]
        self.is_futures = market == "futures"
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
        res = self._call("order.place", binance_order_params(self.cfg, self.is_futures, cl))
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
        arg = {
            "instId": self.cfg["inst_id"], "instIdCode": self.inst_id_code,
            "tdMode": self.cfg["td_mode"],
            "side": self.cfg.get("side", "buy"), "ordType": "limit",
            "px": self.cfg["price"], "sz": self.cfg["size"], "clOrdId": cl,
        }
        if self.cfg.get("pos_side"):                # long/short-режим
            arg["posSide"] = self.cfg["pos_side"]
        item = self._call("order", arg)
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
#                              MEXC (REST)                                    #
# =========================================================================== #
# MEXC — две РАЗНЫЕ системы, обе на api.mexc.com: спот (Binance-совместимый
# /api/v3, подпись X-MEXC-APIKEY) и контракты (/api/v1/private, своя подпись
# ApiKey+Request-Time+Signature = HMAC(accessKey+ts+body)). Фьючерсный API
# MEXC был «Under maintenance», но СНОВА ОТКРЫТ (перезапуск 31.03.2026): place
# order = POST /api/v1/private/order/create, trigger = planorder/place/v2.
# Требуется KYC + право Futures API trading у ключа, иначе придёт ошибка прав.
# Условные ордера у MEXC есть только на фьючерсах. WS-API ордеров у MEXC нет —
# замер только REST. Лимит контрактного order-API: 4 запроса / 2 с.
MEXC_SPOT_BASE = "https://api.mexc.com"
MEXC_CONTRACT_BASE = "https://api.mexc.com"


def mexc_contract_params(cfg, cl):
    """Тело контрактного ордера MEXC. side: 1 open long / 3 open short. type:
    1 limit … 5 market. Для trigger добавляются triggerPrice/triggerType/trend/
    executeCycle/orderType и запрос идёт на planorder/place."""
    side = str(cfg.get("side", "buy")).lower()
    otype = int(cfg.get("type", 1))
    body = {
        "symbol": cfg["symbol"],                     # формат BTC_USDT
        "vol": float(cfg["size"]),
        "side": 1 if side.startswith("b") else 3,    # 1 open long, 3 open short
        "type": otype,                               # 1 limit … 5 market
        "openType": int(cfg.get("open_type", 2)),    # 1 isolated, 2 cross
        "externalOid": cl,
    }
    if cfg.get("leverage"):                          # обычный ордер резервирует маржу сразу
        body["leverage"] = int(cfg["leverage"])
    if otype not in (5, 6):                          # лимитным нужна цена
        body["price"] = float(cfg["price"])
    if cfg.get("trigger_price"):                     # trigger → planorder/place
        body["triggerPrice"] = float(cfg["trigger_price"])
        body["triggerType"] = int(cfg.get("trigger_type", 1))   # 1 >=, 2 <=
        body["executeCycle"] = int(cfg.get("execute_cycle", 1))  # 1 24h, 2 7d
        body["trend"] = int(cfg.get("trend", 1))                 # 1 last,2 fair,3 index
        body["orderType"] = int(cfg.get("order_type", 1))        # тип при срабатывании
    return body


class MexcRest:
    def __init__(self, cfg, market):
        self.cfg = cfg
        self.market = market
        self.is_futures = market == "futures"
        self.key = cfg["api_key"]
        self.secret = cfg["api_secret"].encode()
        self.timeout = cfg.get("timeout_sec", 10)
        self.symbol = cfg["symbol"]
        # условный = фьючерс с триггером (на споте MEXC триггеров нет)
        self.algo = self.is_futures and bool(cfg.get("trigger_price"))
        self.session = requests.Session()

    # ---- спот: api.mexc.com, подпись как у Binance ----
    def _spot(self, method, path, params):
        params = dict(params)
        params["timestamp"] = now_ms()
        params["recvWindow"] = self.cfg.get("recv_window", 5000)
        query = urlencode(params)
        sig = hmac.new(self.secret, query.encode(), hashlib.sha256).hexdigest()
        url = f"{MEXC_SPOT_BASE}{path}?{query}&signature={sig}"
        r = self.session.request(method, url, headers={"X-MEXC-APIKEY": self.key},
                                 timeout=self.timeout)
        try:
            data = r.json()
        except ValueError:
            raise RuntimeError(f"MEXC spot {r.status_code}: не-JSON {r.text[:200]!r}")
        if r.status_code != 200:
            raise RuntimeError(f"MEXC spot {r.status_code}: {data}")
        return data

    # ---- контракты: contract.mexc.com, подпись ApiKey+ts+body ----
    def _contract(self, method, path, body=None, query=None):
        ts = str(now_ms())
        headers = {"ApiKey": self.key, "Request-Time": ts,
                   "Content-Type": "application/json"}
        if method == "GET":
            qs = urlencode(dict(sorted((query or {}).items())))
            headers["Signature"] = hmac.new(
                self.secret, (self.key + ts + qs).encode(), hashlib.sha256).hexdigest()
            url = f"{MEXC_CONTRACT_BASE}{path}" + (f"?{qs}" if qs else "")
            r = self.session.get(url, headers=headers, timeout=self.timeout)
        else:
            body_str = json.dumps(body if body is not None else {})
            headers["Signature"] = hmac.new(
                self.secret, (self.key + ts + body_str).encode(), hashlib.sha256).hexdigest()
            r = self.session.request(method, f"{MEXC_CONTRACT_BASE}{path}",
                                     data=body_str, headers=headers, timeout=self.timeout)
        try:
            data = r.json()
        except ValueError:
            raise RuntimeError(f"MEXC contract {r.status_code}: не-JSON {r.text[:200]!r}")
        if data.get("code") not in (0, 200):        # 500 = Under maintenance
            raise RuntimeError(f"MEXC contract code={data.get('code')}: "
                               f"{data.get('message') or data}")
        return data

    def place_order(self):
        cl = gen_cl_id()
        if not self.is_futures:                      # СПОТ (лимит)
            data = self._spot("POST", "/api/v3/order", {
                "symbol": self.symbol, "side": str(self.cfg.get("side", "BUY")).upper(),
                "type": "LIMIT", "quantity": self.cfg["size"],
                "price": self.cfg["price"], "newClientOrderId": cl,
            })
            if not data.get("orderId"):
                raise RuntimeError(f"ордер не размещён: {data}")
            return cl                                # отмена по clientOrderId
        # Новый (переоткрытый) фьючерсный API: order/create, trigger → planorder/place/v2
        path = "/api/v1/private/planorder/place/v2" if self.algo else "/api/v1/private/order/create"
        data = self._contract("POST", path, mexc_contract_params(self.cfg, cl))
        oid = data.get("data")
        if isinstance(oid, dict):                    # order/create: data={orderId, ts}
            oid = oid.get("orderId")                 # planorder/place/v2: data=строка orderId
        if not oid:
            raise RuntimeError(f"ордер не размещён: {data}")
        return oid                                   # отмена по orderId

    def cancel_order(self, oid):
        if not self.is_futures:                      # СПОТ
            self._spot("DELETE", "/api/v3/order",
                       {"symbol": self.symbol, "origClientOrderId": oid})
            return
        if self.algo:
            self._contract("POST", "/api/v1/private/planorder/cancel",
                           [{"symbol": self.symbol, "orderId": oid}])
        else:
            self._contract("POST", "/api/v1/private/order/cancel", [oid])

    def available_usdt(self):
        if not self.is_futures:
            data = self._spot("GET", "/api/v3/account", {})
            for b in data.get("balances", []):
                if b.get("asset") == "USDT":
                    return float(b.get("free", 0) or 0)
            return 0.0
        data = self._contract("GET", "/api/v1/private/account/asset/USDT")
        d = data.get("data") or {}
        return float(d.get("availableBalance", d.get("availableMargin", 0)) or 0)

    def list_open(self):
        if not self.is_futures:
            return self._spot("GET", "/api/v3/openOrders", {"symbol": self.symbol})
        if self.algo:
            d = self._contract("GET", "/api/v1/private/planorder/list/orders",
                               query={"symbol": self.symbol})
        else:
            d = self._contract("GET", f"/api/v1/private/order/list/open_orders/{self.symbol}")
        return d.get("data") or []

    def position_amt(self):
        if not self.is_futures:
            return 0.0
        try:
            d = self._contract("GET", "/api/v1/private/position/open_positions",
                               query={"symbol": self.symbol})
            return sum(float(p.get("holdVol", 0) or 0) for p in (d.get("data") or []))
        except Exception:
            return 0.0

    def reconcile(self):
        id_field = "externalOid" if self.is_futures else "clientOrderId"
        try:
            ours = [o for o in self.list_open()
                    if str(o.get(id_field, "")).startswith("lt")]
        except Exception:
            ours = []
        for o in ours:
            try:
                self.cancel_order(o.get("orderId") if self.is_futures
                                  else o.get("clientOrderId"))
            except Exception:
                pass
        try:
            left = [o for o in self.list_open()
                    if str(o.get(id_field, "")).startswith("lt")]
        except Exception:
            left = []
        return len(ours), len(left), self.position_amt()


# =========================================================================== #
#               Авто-цена: безопасный неисполняемый лимит                     #
# =========================================================================== #
# Идея: вместо ручной подгонки "price" в конфиге считаем цену от текущего
# рынка. Для BUY берём лучший бид и опускаемся на offset% ниже, для SELL -
# лучший аск плюс offset%. Цена округляется к шагу инструмента (tickSize).
# Так ордер заведомо не пересекает спред (не исполняется) и при этом
# попадает в ценовой коридор биржи (Binance PERCENT_PRICE(_BY_SIDE),
# OKX price-limit), из-за которого фиксированная заглушка "20000" отклоняется.
# Все запросы цены идут ДО таймера и на сам замер задержки не влияют.

_price_cache = {}


def auto_price_on(cfg):
    """Включена ли авто-цена: CLI-флаг / env / ключ в конфиге."""
    if "--auto-price" in sys.argv:
        return True
    env = os.environ.get("AUTO_PRICE", "").strip().lower()
    if env in ("1", "yes", "true", "on"):
        return True
    if env in ("0", "no", "false", "off"):
        return False
    return bool(cfg.get("auto_price", False))


def price_offset(cfg):
    """Отступ от рынка в долях (PRICE_OFFSET в процентах; по умолчанию 1%)."""
    val = os.environ.get("PRICE_OFFSET")
    if val is None:
        val = cfg.get("price_offset_pct", 1.0)
    try:
        return float(val) / 100.0
    except (TypeError, ValueError):
        return 0.01


def _round_to_tick(value, tick, is_buy):
    """Округляем к шагу цены: BUY вниз, SELL вверх (чтобы не приблизиться к рынку)."""
    v = Decimal(str(value))
    t = Decimal(str(tick))
    if t <= 0:
        return format(v, "f")
    steps = (v / t).to_integral_value(rounding=ROUND_DOWN if is_buy else ROUND_UP)
    price = steps * t
    exp = t.as_tuple().exponent
    decimals = -exp if exp < 0 else 0
    return f"{price:.{decimals}f}"


def _compute_price(cfg, best_bid, best_ask, tick, offset):
    if best_bid <= 0 or best_ask <= 0:
        raise RuntimeError(f"некорректные лучшие цены: bid={best_bid} ask={best_ask}")
    is_buy = str(cfg.get("side", "BUY")).upper().startswith("B")
    if is_buy:
        raw = best_bid * (1.0 - offset)
    else:
        raw = best_ask * (1.0 + offset)
    return _round_to_tick(raw, tick, is_buy)


def _binance_public_get(url, params, timeout):
    """GET к публичному Binance с понятной ошибкой при гео-блокировке (451)."""
    r = requests.get(url, params=params, timeout=timeout)
    if r.status_code == 451:
        raise RuntimeError("Binance недоступен из этого региона (HTTP 451, "
                           "гео-ограничение; для США — отдельный binance.us)")
    if r.status_code != 200:
        raise RuntimeError(f"Binance API {r.status_code}: {r.text[:160]}")
    return r.json()


def binance_safe_price(cfg, market, offset):
    base = cfg["base_url"].rstrip("/")
    symbol = cfg["symbol"]
    timeout = cfg.get("timeout_sec", 10)
    is_futures = market == "futures"
    book_path = "/fapi/v1/ticker/bookTicker" if is_futures else "/api/v3/ticker/bookTicker"
    info_path = "/fapi/v1/exchangeInfo" if is_futures else "/api/v3/exchangeInfo"

    bt = _binance_public_get(base + book_path, {"symbol": symbol}, timeout)
    best_bid, best_ask = float(bt["bidPrice"]), float(bt["askPrice"])

    if is_futures:
        # fapi exchangeInfo отдаёт все символы - фильтруем сами.
        info = _binance_public_get(base + info_path, None, timeout)
        sym = next(s for s in info["symbols"] if s["symbol"] == symbol)
    else:
        info = _binance_public_get(base + info_path, {"symbol": symbol}, timeout)
        sym = info["symbols"][0]
    tick = next(f["tickSize"] for f in sym["filters"] if f["filterType"] == "PRICE_FILTER")
    return _compute_price(cfg, best_bid, best_ask, tick, offset)


def okx_safe_price(cfg, offset):
    base = cfg["base_url"].rstrip("/")
    inst = cfg["inst_id"]
    timeout = cfg.get("timeout_sec", 10)
    headers = {}
    if cfg.get("simulated", False):
        headers["x-simulated-trading"] = "1"

    tk = requests.get(base + "/api/v5/market/ticker", params={"instId": inst},
                      headers=headers, timeout=timeout).json()
    if tk.get("code") != "0" or not tk.get("data"):
        raise RuntimeError(f"OKX ticker: {tk}")
    d = tk["data"][0]
    best_bid, best_ask = float(d["bidPx"]), float(d["askPx"])

    inst_type = "SWAP" if inst.endswith("-SWAP") else "SPOT"
    ins = requests.get(base + "/api/v5/public/instruments",
                       params={"instType": inst_type, "instId": inst},
                       headers=headers, timeout=timeout).json()
    if ins.get("code") != "0" or not ins.get("data"):
        raise RuntimeError(f"OKX instruments: {ins}")
    tick = ins["data"][0]["tickSz"]
    return _compute_price(cfg, best_bid, best_ask, tick, offset)


def _auto_price(key, fn):
    """Считаем цену один раз на рынок (для API и WS), печатаем один раз."""
    if key not in _price_cache:
        price = fn()
        _price_cache[key] = price
        print(f"  [auto-price] {key[0]} {key[1]}: {price}")
    return _price_cache[key]


# =========================================================================== #
#             Авто-размер: минимальный валидный объём ордера                  #
# =========================================================================== #
# Берём с биржи минимальный размер и шаг, проверяем минимальный номинал и
# отдаём наименьший объём, который биржа примет. Так «минимальный размер»
# из чек-листа выполняется точно, без ручного подбора. Для OKX SWAP размер
# считается в КОНТРАКТАХ (1 контракт = ctVal базовой валюты), не в BTC.

_size_cache = {}


def auto_size_on(cfg):
    if "--auto-size" in sys.argv:
        return True
    env = os.environ.get("AUTO_SIZE", "").strip().lower()
    if env in ("1", "yes", "true", "on"):
        return True
    if env in ("0", "no", "false", "off"):
        return False
    return bool(cfg.get("auto_size", False))


def _round_up_to_step(value, step):
    v = Decimal(str(value))
    s = Decimal(str(step))
    if s <= 0:
        return v
    return (v / s).to_integral_value(rounding=ROUND_UP) * s


def _fmt_step(value, step):
    s = Decimal(str(step))
    exp = s.as_tuple().exponent
    decimals = -exp if exp < 0 else 0
    return f"{Decimal(str(value)):.{decimals}f}"


def _min_qty_for_notional(price, min_qty, step, min_notional):
    """Binance: max(minQty, minNotional/price), округлённый ВВЕРХ к stepSize."""
    price = Decimal(str(price))
    q = Decimal(str(min_qty))
    if min_notional and price > 0:
        need = Decimal(str(min_notional)) / price
        if need > q:
            q = need
    q = _round_up_to_step(q, step)
    floor_q = _round_up_to_step(Decimal(str(min_qty)), step)
    if q < floor_q:
        q = floor_q
    return _fmt_step(q, step)


def _okx_min_size(min_sz, lot_sz):
    """OKX: минимальный размер minSz, округлённый вверх к шагу lotSz."""
    size = _round_up_to_step(Decimal(str(min_sz)), lot_sz)
    if size < Decimal(str(min_sz)):
        size = _round_up_to_step(Decimal(str(min_sz)), lot_sz)
    return _fmt_step(size, lot_sz)


def binance_min_order_size(cfg, market):
    base = cfg["base_url"].rstrip("/")
    symbol = cfg["symbol"]
    timeout = cfg.get("timeout_sec", 10)
    is_futures = market == "futures"
    info_path = "/fapi/v1/exchangeInfo" if is_futures else "/api/v3/exchangeInfo"
    if is_futures:
        info = _binance_public_get(base + info_path, None, timeout)
        sym = next(s for s in info["symbols"] if s["symbol"] == symbol)
    else:
        info = _binance_public_get(base + info_path, {"symbol": symbol}, timeout)
        sym = info["symbols"][0]
    filt = {f["filterType"]: f for f in sym["filters"]}
    lot = filt.get("LOT_SIZE", {})
    min_qty = lot.get("minQty", "0")
    step = lot.get("stepSize", "0")
    notf = filt.get("MIN_NOTIONAL") or filt.get("NOTIONAL") or {}
    min_notional = notf.get("minNotional") or notf.get("notional") or 0
    return _min_qty_for_notional(float(cfg["price"]), min_qty, step, min_notional)


def okx_min_order_size(cfg):
    base = cfg["base_url"].rstrip("/")
    inst = cfg["inst_id"]
    timeout = cfg.get("timeout_sec", 10)
    headers = {}
    if cfg.get("simulated", False):
        headers["x-simulated-trading"] = "1"
    inst_type = "SWAP" if inst.endswith("-SWAP") else "SPOT"
    ins = requests.get(base + "/api/v5/public/instruments",
                       params={"instType": inst_type, "instId": inst},
                       headers=headers, timeout=timeout).json()
    if ins.get("code") != "0" or not ins.get("data"):
        raise RuntimeError(f"OKX instruments: {ins}")
    d = ins["data"][0]
    min_sz = d.get("minSz", "0")
    lot_sz = d.get("lotSz", "0") or min_sz
    return _okx_min_size(min_sz, lot_sz)


def _auto_size(key, fn):
    if key not in _size_cache:
        size = fn()
        _size_cache[key] = size
        print(f"  [auto-size] {key[0]} {key[1]}: {size}")
    return _size_cache[key]


def _arg(name, default=None):
    """Чтение --name value или --name=value из argv."""
    pref = "--" + name
    for i, a in enumerate(sys.argv):
        if a == pref and i + 1 < len(sys.argv):
            return sys.argv[i + 1]
        if a.startswith(pref + "="):
            return a.split("=", 1)[1]
    return default


def _flag(name, env, default=False):
    if "--" + name in sys.argv:
        return True
    val = os.environ.get(env, "").strip().lower()
    if val in ("1", "yes", "true", "on"):
        return True
    if val in ("0", "no", "false", "off"):
        return False
    return default


# =========================================================================== #
#        Условный режим, balance-guard и проверка серверного времени          #
# =========================================================================== #
# Условный (trigger) бенчмарк включается флагом --conditional / CONDITIONAL=1 и
# использует ОТДЕЛЬНЫЙ конфиг Binance (CONFIG_BINANCE_CONDITIONAL). Ордер не может
# сработать (триггер недостижим), но размер крупный — поэтому добавлен balance-guard:
# перед размещением проверяем свободный баланс и ОТКАЗЫВАЕМ, если на счёте достаточно
# средств, чтобы условие реально открыло позицию. Это снимает единственный реальный
# риск такой схемы — «безопасно только на пустом счёте, опасно после пополнения».

_guard_cache = {}


def conditional_on():
    return _flag("conditional", "CONDITIONAL", False)


def balance_guard_on():
    return _flag("no-balance-guard", "NO_BALANCE_GUARD", False) is False \
        and _flag("balance-guard", "BALANCE_GUARD", True)


def max_equity_usdt():
    try:
        return float(os.environ.get("MAX_EQUITY_USDT", "50"))
    except ValueError:
        return 50.0


def balance_guard(cfg, market, exch="Binance"):
    """Пускаем условный ордер только если на счёте слишком мало средств, чтобы он
    мог что-то открыть. Иначе — стоп с явным сообщением."""
    key = (exch, market)
    ceil = max_equity_usdt()
    fresh = key not in _guard_cache
    if fresh:
        if exch == "Binance":
            client = BinanceRest(cfg, market)
        elif exch == "OKX":
            client = OkxRest(cfg)
        else:
            client = MexcRest(cfg, market)
        try:
            _guard_cache[key] = client.available_usdt()
        except Exception as e:                      # баланс не прочитать (напр. maintenance)
            print(f"  [balance-guard] {exch} {market}: не удалось прочитать баланс "
                  f"({e}) — пропускаю (триггер всё равно недостижим)")
            _guard_cache[key] = None
    avail = _guard_cache[key]
    if avail is None:                               # fail-open: читать нечем
        return
    if avail > ceil:
        size = cfg.get("quantity") or cfg.get("size")
        inst = cfg.get("symbol") or cfg.get("inst_id")
        raise RuntimeError(
            f"balance-guard: доступно {avail:.2f} USDT > порога {ceil:.0f}. "
            f"Условный ордер {size} {inst} на пополненном счёте может открыть позицию. "
            f"Отключить: BALANCE_GUARD=0 / --no-balance-guard, или поднять порог "
            f"MAX_EQUITY_USDT (триггер всё равно недостижим).")
    if fresh:
        print(f"  [balance-guard] {exch} {market}: доступно {avail:.2f} USDT ≤ {ceil:.0f} — ок")


_pos_side_cache = {}


def resolve_position_side(cfg, market):
    """Binance Futures в hedge-режиме требует positionSide в каждом ордере (иначе
    -4061). Определяем режим счёта один раз и выводим positionSide из стороны
    ордера. В one-way режиме возвращаем None (поле не нужно). Конфиг может задать
    position_side явно — тогда детект пропускаем."""
    if market != "futures":
        return None
    if cfg.get("position_side"):
        return cfg["position_side"]
    key = ("Binance", market)
    if key not in _pos_side_cache:
        try:
            hedge = BinanceRest(cfg, market).dual_side_position()
        except Exception as e:
            print(f"  [pos-mode] Binance {market}: не удалось определить ({e}) — без positionSide")
            _pos_side_cache[key] = None
            return None
        if hedge:
            side = str(cfg.get("side", "BUY")).upper()
            ps = "LONG" if side.startswith("B") else "SHORT"
        else:
            ps = None
        _pos_side_cache[key] = ps
        mode = f"hedge → positionSide={ps}" if ps else "one-way (positionSide не нужен)"
        print(f"  [pos-mode] Binance {market}: {mode}")
    return _pos_side_cache[key]


def resolve_okx_pos_side(cfg):
    """OKX в режиме long/short (hedge) требует posSide=long/short в каждом ордере
    (иначе 51000 «Parameter posSide error»). В net-режиме posSide не нужен.
    Определяем режим счёта один раз через /api/v5/account/config. Конфиг может
    задать pos_side явно — тогда детект пропускаем."""
    if cfg.get("pos_side"):
        return cfg["pos_side"]
    key = "OKX"
    if key not in _pos_side_cache:
        try:
            data = OkxRest(cfg)._request("GET", "/api/v5/account/config", None)["data"]
            pos_mode = data[0].get("posMode") if data else "net_mode"
        except Exception as e:
            print(f"  [pos-mode] OKX: не удалось определить ({e}) — без posSide")
            _pos_side_cache[key] = None
            return None
        if pos_mode == "long_short_mode":
            side = str(cfg.get("side", "buy")).lower()
            ps = "long" if side.startswith("b") else "short"
        else:
            ps = None
        _pos_side_cache[key] = ps
        mode = f"long/short → posSide={ps}" if ps else "net (posSide не нужен)"
        print(f"  [pos-mode] OKX futures: {mode}")
    return _pos_side_cache[key]


def server_time_ms(exch, base, is_futures, simulated, timeout):
    base = base.rstrip("/")
    if exch == "binance":
        path = "/fapi/v1/time" if is_futures else "/api/v3/time"
        d = _binance_public_get(base + path, None, timeout)
        return int(d["serverTime"])
    if exch == "mexc":                               # сверяем по споту (api.mexc.com)
        d = requests.get(MEXC_SPOT_BASE + "/api/v3/time", timeout=timeout).json()
        return int(d["serverTime"])
    headers = {"x-simulated-trading": "1"} if simulated else {}
    d = requests.get(base + "/api/v5/public/time", headers=headers, timeout=timeout).json()
    return int(d["data"][0]["ts"])


def time_sync_preflight(markets):
    """Сверяем локальные часы с временем биржи: кривое время и портит замер,
    и вызывает reject по recvWindow. Проверяем по одному разу на биржу."""
    if _flag("skip-time-check", "SKIP_TIME_CHECK", False):
        return
    warn_ms, hard_ms = 500, 3000
    seen = set()
    for label, fn, market, exch in markets:
        if exch in seen:
            continue
        seen.add(exch)
        try:
            cfg = {"binance": binance_cfg, "okx": okx_cfg,
                   "mexc": mexc_cfg}[exch](market)
            t0 = now_ms()
            srv = server_time_ms(exch, cfg.get("base_url", ""), market == "futures",
                                 cfg.get("simulated", False), cfg.get("timeout_sec", 10))
            t1 = now_ms()
            offset = srv - (t0 + t1) // 2
        except Exception as e:
            print(f"  [time] {exch}: не удалось проверить ({e}) — пропускаю")
            continue
        a = abs(offset)
        if a > hard_ms:
            print(f"  [time] {exch}: offset {offset:+d} мс — СЛИШКОМ БОЛЬШОЙ (> {hard_ms}). "
                  f"Синхронизируйте часы (README) или --skip-time-check.")
            sys.exit(1)
        elif a > warn_ms:
            print(f"  [time] {exch}: offset {offset:+d} мс — ⚠ велик (> {warn_ms}), "
                  f"замер может искажаться")
        else:
            print(f"  [time] {exch}: offset {offset:+d} мс — ок")


# =========================================================================== #
#                                 Замер                                       #
# =========================================================================== #
REPEATS = int(os.environ.get("LATENCY_REPEATS", "5"))  # циклов "отправка+отмена"
# Пауза между REST и WS прогонами одного рынка (вне замера). Даёт окну rate-limit
# биржи сброситься — без неё OKX отдаёт 50011 на WS сразу после REST (особенно
# при нулевом fill-ratio, как в этом бенчмарке). Поставьте 0, чтобы отключить.
TRANSPORT_PAUSE_SEC = float(os.environ.get("TRANSPORT_PAUSE_SEC", "2"))


def _ms(a, b):
    return (b - a) * 1000.0


def measure(place_fn, cancel_fn, repeats=None):
    """
    1) Первый ордер: меряем только размещение (для REST оно "холодное" -
       включает установку соединения). Его отмену НЕ учитываем - чистим.
    2) На прогретом соединении повторяем repeats раз "отправка+отмена",
       усредняем размещения и отмены по повторам.
    Итого повт. = среднее размещение + средняя отмена.
    """
    if repeats is None:
        repeats = REPEATS
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


def binance_cfg(market):
    path = CONFIG_BINANCE_CONDITIONAL if conditional_on() else CONFIG_BINANCE
    full = load_config(path)
    cfg = dict(full[market])
    for k in ("auto_price", "price_offset_pct", "auto_size"):   # общие ключи конфига
        if k in full and k not in cfg:
            cfg[k] = full[k]
    cfg["api_key"], cfg["api_secret"] = binance_credentials(market)
    return cfg


def okx_cfg(market):
    full = load_config(CONFIG_OKX_CONDITIONAL if conditional_on() else CONFIG_OKX)
    shared = {k: full[k] for k in
              ("base_url", "ws_url", "simulated", "timeout_sec",
               "auto_price", "price_offset_pct", "auto_size")
              if k in full}
    cfg = {**shared, **full[market]}
    cfg["api_key"], cfg["api_secret"], cfg["passphrase"] = okx_credentials()
    return cfg


def mexc_cfg(market):
    full = load_config(CONFIG_MEXC_CONDITIONAL if conditional_on() else CONFIG_MEXC)
    shared = {k: full[k] for k in ("timeout_sec", "auto_price", "price_offset_pct",
                                   "auto_size") if k in full}
    cfg = {**shared, **full[market]}
    cfg["api_key"], cfg["api_secret"] = mexc_credentials()
    return cfg


def run_binance(market, transport):
    cfg = binance_cfg(market)
    if is_conditional(cfg) and balance_guard_on():
        balance_guard(cfg, market)
    if auto_price_on(cfg):
        cfg["price"] = _auto_price(
            ("Binance", market),
            lambda: binance_safe_price(cfg, market, price_offset(cfg)))
    if auto_size_on(cfg):
        cfg["quantity"] = _auto_size(
            ("Binance", market),
            lambda: binance_min_order_size(cfg, market))
    ps = resolve_position_side(cfg, market)         # hedge-режим фьючерсов
    if ps:
        cfg["position_side"] = ps
    if transport == "API":
        c = BinanceRest(cfg, market)
        return measure(c.place_order, c.cancel_order)
    if market == "futures" and is_conditional(cfg):
        # Условные ордера USDⓈ-M futures с 2025-12-09 идут через Algo REST
        # (/fapi/v1/algoOrder); WS API их не принимает (order.place даёт -4120).
        raise RuntimeError("Условные ордера Binance Futures с 2025-12-09 — только "
                           "через Algo REST (/fapi/v1/algoOrder), WS API не поддерживает")
    return measure_ws(BinanceWs(cfg, market))


def run_okx(market, transport):
    cfg = okx_cfg(market)
    if okx_is_conditional(cfg) and balance_guard_on():
        balance_guard(cfg, market, "OKX")
    if market == "futures":                         # long/short-режим требует posSide
        ps = resolve_okx_pos_side(cfg)
        if ps:
            cfg["pos_side"] = ps
    if auto_price_on(cfg):
        cfg["price"] = _auto_price(
            ("OKX", market),
            lambda: okx_safe_price(cfg, price_offset(cfg)))
    if auto_size_on(cfg):
        cfg["size"] = _auto_size(
            ("OKX", market),
            lambda: okx_min_order_size(cfg))
    if transport == "API":
        c = OkxRest(cfg)
        return measure(c.place_order, c.cancel_order)
    if okx_is_conditional(cfg):
        # OKX algo-ордера РАЗМЕЩАЮТСЯ только через REST. WS-канал algo-orders —
        # это подписка на обновления (push), не размещение; op order-algo в WS
        # trade API нет (есть только order/batch-orders/cancel-order/amend-order).
        raise RuntimeError("OKX: algo (trigger) ордера размещаются только через REST "
                           "(WS-канал algo-orders — подписка на обновления, не "
                           "размещение; op order-algo в WS trade API отсутствует)")
    return measure_ws(OkxWs(cfg))


def mexc_safe_price(cfg, offset, market):
    """Безопасная неисполняемая цена MEXC: BUY ниже лучшего бида на offset, SELL
    выше лучшего аска. Спот — bookTicker, фьючерс — contract/ticker (bid1/ask1).
    offset уже доля (не проценты). Округляем к целому (валидный тик для BTC)."""
    timeout = cfg.get("timeout_sec", 10)
    if market == "futures":
        r = requests.get(MEXC_CONTRACT_BASE + "/api/v1/contract/ticker",
                         params={"symbol": cfg["symbol"]}, timeout=timeout).json()
        d = r.get("data") or {}
        bid, ask = float(d.get("bid1") or 0), float(d.get("ask1") or 0)
    else:
        d = requests.get(MEXC_SPOT_BASE + "/api/v3/ticker/bookTicker",
                         params={"symbol": cfg["symbol"]}, timeout=timeout).json()
        bid, ask = float(d["bidPrice"]), float(d["askPrice"])
    return _compute_price(cfg, bid, ask, "1", offset)


def mexc_min_order_size(cfg, market):
    """Минимальный валидный объём MEXC. Фьючерс — contract/detail.minVol (в
    контрактах). Спот — exchangeInfo: max(baseSizePrecision, минНоминал/цена)."""
    timeout = cfg.get("timeout_sec", 10)
    if market == "futures":
        r = requests.get(MEXC_CONTRACT_BASE + "/api/v1/contract/detail",
                         params={"symbol": cfg["symbol"]}, timeout=timeout).json()
        d = r.get("data")
        if isinstance(d, list):
            d = next((x for x in d if x.get("symbol") == cfg["symbol"]), d[0])
        return _okx_min_size(d.get("minVol", "1"), d.get("volUnit", "1") or "1")
    info = requests.get(MEXC_SPOT_BASE + "/api/v3/exchangeInfo",
                        params={"symbol": cfg["symbol"]}, timeout=timeout).json()
    sym = info["symbols"][0]
    base_step = sym.get("baseSizePrecision") or "0.000001"
    min_notional = sym.get("quoteAmountPrecision") or "0"
    return _min_qty_for_notional(float(cfg["price"]), base_step, base_step, min_notional)


def run_mexc(market, transport):
    cfg = mexc_cfg(market)
    if market == "futures" and bool(cfg.get("trigger_price")) and balance_guard_on():
        balance_guard(cfg, market, "MEXC")
    if auto_price_on(cfg):                           # неисполняемая цена от рынка
        cfg["price"] = _auto_price(
            ("MEXC", market), lambda: mexc_safe_price(cfg, price_offset(cfg), market))
    if auto_size_on(cfg):                            # минимальный валидный объём
        cfg["size"] = _auto_size(
            ("MEXC", market), lambda: mexc_min_order_size(cfg, market))
    if transport != "API":
        # У MEXC нет WS-API размещения ордеров (WS — только маркет-дата и
        # user-data стримы). Замер place/cancel — только REST.
        raise RuntimeError("MEXC: размещение ордеров только через REST "
                           "(WS-API ордеров у MEXC нет)")
    c = MexcRest(cfg, market)
    return measure(c.place_order, c.cancel_order)


def reconcile_market(exch, market):
    """Защитная зачистка после прогона: снимаем свои висячие ордера и
    проверяем позицию (на случай reject/таймаута/частичного исполнения)."""
    if exch == "binance":
        c = BinanceRest(binance_cfg(market), market)
        label = f"Binance {market}"
    elif exch == "okx":
        c = OkxRest(okx_cfg(market))
        label = f"OKX {market}"
    else:
        c = MexcRest(mexc_cfg(market), market)
        label = f"MEXC {market}"
    cancelled, left, pos = c.reconcile()
    note = f"снято наших={cancelled}, осталось={left}, позиция={pos:g}"
    if left or abs(pos) > 1e-12:
        print(f"  [reconcile] {label}: ⚠ {note} — ПРОВЕРЬТЕ ВРУЧНУЮ!")
    else:
        print(f"  [reconcile] {label}: ок ({note})")


ALL_MARKETS = [
    ("Binance Futures", run_binance, "futures", "binance"),
    ("Binance Spot",    run_binance, "spot",    "binance"),
    ("OKX Futures",     run_okx,     "futures", "okx"),
    ("OKX Spot",        run_okx,     "spot",    "okx"),
    ("MEXC Futures",    run_mexc,    "futures", "mexc"),
    ("MEXC Spot",       run_mexc,    "spot",    "mexc"),
]


def select_markets(exch, mkt):
    return [row for row in ALL_MARKETS
            if exch in ("both", row[3]) and mkt in ("both", row[2])]


# =========================================================================== #
#                                 Таблица                                     #
# =========================================================================== #
def print_table(results, markets):
    W_LABEL, W_NUM = 18, 15
    line = "─" * (W_LABEL + W_NUM * 4)
    h1 = ("Первый", "Повторный", "Отмена", "Итого")
    h2 = ("ордер", "ордер (ср.)", "ордера (ср.)", "повт. (ср.)")

    kind = "условного (trigger)" if conditional_on() else "лимитного"
    print()
    print(f"  Задержка {kind} ордера (БОЕВОЙ счёт), мс")
    print("  «Повторный» — среднее по {} циклам на прогретом соединении".format(REPEATS))
    print(line)
    print(f"{'':<{W_LABEL}}" + "".join(f"{x:>{W_NUM}}" for x in h1))
    print(f"{'':<{W_LABEL}}" + "".join(f"{x:>{W_NUM}}" for x in h2))
    print(line)

    errors = []
    for label, _, _, _ in markets:
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
                short = msg if len(msg) <= 44 else msg[:44] + "…"
                print(f"{tag:<{W_LABEL}}  ✗ {short}")
                errors.append((f"{label} / {disp}", msg))
    print(line)
    if errors:
        print("\n  Детали ошибок (полностью):")
        for who, msg in errors:
            print(f"  • {who}: {msg}")


def confirm_live(skip):
    """Подтверждение перед торговлей реальными деньгами на PROD."""
    if skip:
        return
    bar = "!" * 70
    print("\n" + bar)
    print("ВНИМАНИЕ: будут размещены РЕАЛЬНЫЕ ордера на БОЕВЫХ счетах (PROD).")
    if conditional_on():
        print("Режим УСЛОВНЫЙ (trigger): крупный ордер с НЕДОСТИЖИМЫМ триггером,")
        print("сразу отменяется. Сработать не может, плюс balance-guard блокирует")
        print("запуск на счёте с достаточной маржой. Но это РЕАЛЬНЫЕ ДЕНЬГИ.")
    else:
        print("Ордера лимитные (BUY ниже / SELL выше рынка), сразу отменяются, но")
        print("это РЕАЛЬНЫЕ ДЕНЬГИ. Убедитесь, что цены/объёмы безопасны (см. README).")
    print(bar)
    try:
        ans = input("Введите 'yes' для продолжения: ").strip().lower()
    except EOFError:
        ans = ""
    if ans not in ("yes", "y", "да"):
        print("Отменено.")
        sys.exit(1)


def main():
    global REPEATS
    skip_confirm = ("--yes" in sys.argv or "-y" in sys.argv
                    or os.environ.get("LATENCY_CONFIRM", "").lower() in ("1", "yes", "true"))
    exch = _arg("exchange", "both").lower()
    mkt = _arg("market", "both").lower()
    reps = _arg("repeats")
    if reps:
        try:
            REPEATS = int(reps)
        except ValueError:
            pass

    markets = select_markets(exch, mkt)
    if not markets:
        print(f"Нет рынков под фильтр exchange={exch} market={mkt}.")
        print("Допустимо: --exchange both|binance|okx|mexc, --market both|spot|futures")
        return

    confirm_live(skip_confirm)
    mode = "УСЛОВНЫЙ (trigger)" if conditional_on() else "лимитный"
    print(f"Замер задержки (БОЕВЫЕ счета): режим={mode}, exchange={exch}, "
          f"market={mkt}, repeats={REPEATS}. Прогон может занять несколько секунд...")
    time_sync_preflight(markets)
    results = {}
    for label, fn, market, exch_name in markets:
        row = {}
        for ti, transport in enumerate(("API", "WS")):
            if ti > 0 and TRANSPORT_PAUSE_SEC > 0:   # дать rate-limit окну сброситься
                time.sleep(TRANSPORT_PAUSE_SEC)
            try:
                row[transport] = fn(market, transport)
            except Exception as e:
                row[transport] = e
        results[label] = row
        try:
            reconcile_market(exch_name, market)
        except Exception as e:
            print(f"  [reconcile] {label}: ✗ {e}")
    print_table(results, markets)


if __name__ == "__main__":
    main()
