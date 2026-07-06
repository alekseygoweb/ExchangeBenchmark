#!/usr/bin/env python3
"""
Публичный замер задержки до бирж — БЕЗ ключей и БЕЗ ордеров.

Отвечает на вопрос «куда ставить торговый сервер», меряя по ПУБЛИЧНЫМ
эндпоинтам (аутентификация не нужна, ордера не размещаются, риска нет):

    REST RTT       — полный цикл GET к публичному эндпоинту (server time / ping)
    WS-upgrade      — установка WebSocket-соединения (DNS+TCP+TLS+HTTP 101)
    Подписка        — от отправки subscribe до подтверждения/первого сообщения
    Ping/pong RTT   — app-level ping→pong на прогретом сокете (где биржа
                      поддерживает; иначе — управляющий WS-ping RFC6455)

Плюс резолв IP + reverse-DNS с подсказкой региона/CDN — чтобы отличить
матчинг-движок (AWS ec2-*.<region>) от CDN-edge (CloudFront/Cloudflare/Akamai).

ВАЖНО про CDN. Публичные REST-эндпоинты бирж стоят за CDN, поэтому REST RTT и
TCP/TLS часто меряют ближайший edge, а не движок. WS-подписка и app-level
ping/pong идут до origin-шлюза (после апгрейда соединение туннелируется до
биржи), поэтому для выбора региона ориентируйтесь в первую очередь на них.

Запуск (на VPS, где будете торговать — Токио/Сингапур/us-east-1 и т.д.):
    pip install -r requirements.txt
    python3 public_latency.py                      # все биржи
    python3 public_latency.py --exchange bybit,mexc,bingx
    python3 public_latency.py --resolve-only        # только IP/rDNS/регион
    python3 public_latency.py --breakdown           # + DNS/TCP/TLS раздельно
"""

import argparse
import gzip
import json
import re
import socket
import ssl
import statistics
import sys
import time
import uuid
from urllib.parse import urlparse

try:
    import requests
except ImportError:
    requests = None

try:
    from websocket import create_connection, ABNF
    from websocket._exceptions import WebSocketTimeoutException
    WS_OK = True
except ImportError:
    WS_OK = False

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")


def _json(text):
    try:
        return json.loads(text)
    except Exception:
        return None


# =========================================================================== #
#            Предикаты подтверждения подписки и pong по биржам                 #
# =========================================================================== #
# Каждый предикат принимает (obj, text): obj — распарсенный JSON (или None),
# text — сырой текст кадра. Возвращает True, когда это ожидаемый ответ.

def _ack_binance(o, t):      # {"result":null,"id":1}
    return isinstance(o, dict) and o.get("id") == 1 and "result" in o

def _pong_binance(o, t):     # ответ на LIST_SUBSCRIPTIONS {"result":[...],"id":2}
    return isinstance(o, dict) and o.get("id") == 2 and "result" in o

def _ack_okx(o, t):          # {"event":"subscribe",...}
    return isinstance(o, dict) and o.get("event") == "subscribe"

def _pong_text(o, t):        # OKX/Bitget: сервер отвечает текстом "pong"
    return t.strip().lower() == "pong"

def _ack_bybit(o, t):        # {"success":true,"op":"subscribe",...}
    return isinstance(o, dict) and (o.get("op") == "subscribe"
                                    or o.get("ret_msg") == "subscribe")

def _pong_bybit(o, t):       # {"op":"pong",...} или {"ret_msg":"pong"}
    return isinstance(o, dict) and (o.get("op") == "pong"
                                    or o.get("ret_msg") == "pong")

def _ack_bitget(o, t):       # {"event":"subscribe","arg":{...}}
    return isinstance(o, dict) and o.get("event") == "subscribe"

def _ack_mexc_spot(o, t):    # {"id":0,"code":0,"msg":"spot@..."}
    return isinstance(o, dict) and o.get("code") == 0

def _pong_mexc_spot(o, t):   # {"id":0,"code":0,"msg":"PONG"}
    return "pong" in t.lower()

def _ack_mexc_fut(o, t):     # {"channel":"rs.sub.ticker",...} / push.ticker
    ch = o.get("channel", "") if isinstance(o, dict) else ""
    return ch.startswith("rs.sub") or ch == "push.ticker"

def _pong_mexc_fut(o, t):    # {"channel":"pong",...}
    return isinstance(o, dict) and o.get("channel") == "pong"

def _ack_bingx(o, t):        # ack {"code":0,...} или первый data-кадр
    return isinstance(o, dict) and (o.get("code") == 0 or o.get("data") is not None)

def _ack_coinbase(o, t):     # {"type":"subscriptions",...}
    return isinstance(o, dict) and o.get("type") == "subscriptions"

def _ack_upbit(o, t):        # Upbit не шлёт ack — сразу поток; ловим первый ticker
    return isinstance(o, dict) and (o.get("type") == "ticker" or o.get("code"))

def _pong_upbit(o, t):       # ответ на "PING": {"status":"UP"}
    return isinstance(o, dict) and o.get("status") == "UP"


# =========================================================================== #
#                        Спецификации бирж                                    #
# =========================================================================== #
# gzip=True — кадры сжаты (BingX). ping=None — app-level ping отсутствует,
# используем управляющий WS-ping RFC6455 (может отвечать CDN-edge — см. легенду).

def _spec(**kw):
    kw.setdefault("gzip", False)
    kw.setdefault("ping", None)
    kw.setdefault("pong", lambda o, t: False)
    kw.setdefault("note", "")
    return kw


SPECS = [
    _spec(key="coinbase", name="Coinbase (spot)", market="spot",
          rest="https://api.exchange.coinbase.com/time",
          ws="wss://ws-feed.exchange.coinbase.com",
          sub=json.dumps({"type": "subscribe", "product_ids": ["BTC-USD"],
                          "channels": ["ticker"]}),
          ack=_ack_coinbase, ping=None,
          note="REST/WS за Cloudflare; ping — управляющий (RFC6455)"),

    _spec(key="coinbase-direct", name="Coinbase direct (spot)", market="spot",
          rest="https://api.exchange.coinbase.com/time",
          ws="wss://ws-direct.exchange.coinbase.com",
          sub=json.dumps({"type": "subscribe", "product_ids": ["BTC-USD"],
                          "channels": ["ticker"]}),
          ack=_ack_coinbase, ping=None,
          note="прямой (не-CDN) фид — резолвится в реальный us-east-1"),

    _spec(key="binanceus", name="Binance.US (spot)", market="spot",
          rest="https://api.binance.us/api/v3/ping",
          ws="wss://stream.binance.us:9443/ws",
          sub=json.dumps({"method": "SUBSCRIBE",
                          "params": ["btcusdt@bookTicker"], "id": 1}),
          ack=_ack_binance, ping=None,
          note="ping — управляющий WS-ping (RFC6455)"),

    _spec(key="bybit", name="Bybit (futures)", market="futures",
          rest="https://api.bybit.com/v5/market/time",
          ws="wss://stream.bybit.com/v5/public/linear",
          sub=json.dumps({"op": "subscribe", "args": ["tickers.BTCUSDT"]}),
          ack=_ack_bybit, ping=json.dumps({"op": "ping"}), pong=_pong_bybit,
          note="AWS Singapore (ap-southeast-1), AZ apse1-az2/az3"),

    _spec(key="bybit-spot", name="Bybit (spot)", market="spot",
          rest="https://api.bybit.com/v5/market/time",
          ws="wss://stream.bybit.com/v5/public/spot",
          sub=json.dumps({"op": "subscribe", "args": ["tickers.BTCUSDT"]}),
          ack=_ack_bybit, ping=json.dumps({"op": "ping"}), pong=_pong_bybit),

    _spec(key="mexc", name="MEXC (spot)", market="spot",
          rest="https://api.mexc.com/api/v3/ping",
          ws="wss://wbs.mexc.com/ws",
          sub=json.dumps({"method": "SUBSCRIPTION",
                          "params": ["spot@public.bookTicker.v3.api@BTCUSDT"]}),
          ack=_ack_mexc_spot, ping=json.dumps({"method": "PING"}),
          pong=_pong_mexc_spot,
          note="маркет-дата в protobuf; ack/PONG — JSON"),

    _spec(key="mexc-futures", name="MEXC (futures)", market="futures",
          rest="https://contract.mexc.com/api/v1/contract/ping",
          ws="wss://contract.mexc.com/edge",
          sub=json.dumps({"method": "sub.ticker", "param": {"symbol": "BTC_USDT"}}),
          ack=_ack_mexc_fut, ping=json.dumps({"method": "ping"}),
          pong=_pong_mexc_fut),

    _spec(key="bingx", name="BingX (futures)", market="futures",
          rest="https://open-api.bingx.com/openApi/swap/v2/server/time",
          ws="wss://open-api-ws.bingx.com/market",
          sub=json.dumps({"id": "sub-btc", "reqType": "sub",
                          "dataType": "BTC-USDT@trade"}),
          ack=_ack_bingx, ping=None, gzip=True,
          note="кадры gzip; ping — управляющий (может отвечать CDN-edge)"),

    _spec(key="bitget", name="Bitget (futures)", market="futures",
          rest="https://api.bitget.com/api/v2/public/time",
          ws="wss://ws.bitget.com/v2/ws/public",
          sub=json.dumps({"op": "subscribe", "args": [
              {"instType": "USDT-FUTURES", "channel": "ticker", "instId": "BTCUSDT"}]}),
          ack=_ack_bitget, ping="ping", pong=_pong_text,
          note="REST за Cloudflare; WS — CloudFront"),

    _spec(key="bitget-spot", name="Bitget (spot)", market="spot",
          rest="https://api.bitget.com/api/v2/public/time",
          ws="wss://ws.bitget.com/v2/ws/public",
          sub=json.dumps({"op": "subscribe", "args": [
              {"instType": "SPOT", "channel": "ticker", "instId": "BTCUSDT"}]}),
          ack=_ack_bitget, ping="ping", pong=_pong_text),

    _spec(key="upbit", name="Upbit (spot, KRW)", market="spot",
          rest="https://api.upbit.com/v1/ticker?markets=KRW-BTC",
          ws="wss://api.upbit.com/websocket/v1",
          sub=json.dumps([{"ticket": "lt-probe"},
                          {"type": "ticker", "codes": ["KRW-BTC"]}]),
          ack=_ack_upbit, ping=None, pace=0.25,
          note="AWS Seoul (ap-northeast-2), прямой EC2 (без CDN); WS-кадры бинарные JSON. "
               "pace=0.25 — rate-limit (429); ping — управляющий RFC6455 "
               "(на текст PING Upbit сразу не отвечает); ориентир — Подписка"),

    # --- Референс: биржи, уже поддержанные в latency_test.py ---
    _spec(key="binance", name="Binance global (spot)", market="spot",
          rest="https://api.binance.com/api/v3/ping",
          ws="wss://stream.binance.com:9443/ws",
          sub=json.dumps({"method": "SUBSCRIBE",
                          "params": ["btcusdt@bookTicker"], "id": 1}),
          ack=_ack_binance, ping=None,
          note="AWS Tokyo (ap-northeast-1); из США REST даёт 451"),

    _spec(key="okx", name="OKX (futures)", market="futures",
          rest="https://www.okx.com/api/v5/public/time",
          ws="wss://ws.okx.com:8443/ws/v5/public",
          sub=json.dumps({"op": "subscribe",
                          "args": [{"channel": "tickers", "instId": "BTC-USDT-SWAP"}]}),
          ack=_ack_okx, ping="ping", pong=_pong_text, pace=0.1,
          note="AWS ap-east-1 (Гонконг); pace=0.1 — REST rate-limit 50011"),
]


# =========================================================================== #
#                    Резолв адреса и подсказка региона                        #
# =========================================================================== #
def _region_hint(ip, rdns):
    r = (rdns or "").lower()
    if "compute-1.amazonaws.com" in r:
        return "AWS us-east-1 (N. Virginia)"
    m = re.search(r"\.([a-z]{2}-[a-z]+-\d)\.compute\.amazonaws\.com", r)
    if m:
        return f"AWS {m.group(1)}"
    if "cloudfront.net" in r:
        return "CloudFront CDN edge (не origin!)"
    if "akamaitechnologies" in r or "akamai" in r:
        return "Akamai CDN edge (не origin!)"
    if re.match(r"^(104\.1[6-9]|104\.2[0-7]|172\.6[4-9]|172\.7[0-1])\.", ip):
        return "Cloudflare CDN edge (не origin!)"
    return "?"


def _resolve(host):
    """Возвращает (ip, rdns) для первого A-адреса хоста."""
    ip = socket.gethostbyname(host)
    try:
        rdns = socket.gethostbyaddr(ip)[0]
    except Exception:
        rdns = None
    return ip, rdns


def _host_port(url):
    u = urlparse(url)
    port = u.port or (443 if u.scheme in ("https", "wss") else 80)
    return u.hostname, port


# =========================================================================== #
#                         Замеры (мс)                                         #
# =========================================================================== #
def _stat(times):
    if not times:
        return None
    return min(times), statistics.median(times)


def _oneline(s, limit=90):
    """Схлопнуть многострочную ошибку в одну строку и распознать гео-блок."""
    s = re.sub(r"\s+", " ", str(s)).strip()
    low = s.lower()
    if "451" in s or "unavailable from a restricted" in low:
        return "гео-блок (HTTP 451, недоступно из этого региона)"
    if "403" in s and ("cloudfront" in low or "block" in low or "country" in low):
        return "гео-блок (HTTP 403, регион заблокирован биржей)"
    return s if len(s) <= limit else s[:limit] + "…"


def _fmt(pair):
    if pair is None:
        return "—"
    mn, md = pair
    return f"{mn:.1f}/{md:.1f}"


def measure_rest(spec, n, timeout):
    """REST RTT: прогреваем сессию, затем n замеров GET (min/median)."""
    if requests is None:
        raise RuntimeError("нет requests")
    pace = spec.get("pace", 0)
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    s.get(spec["rest"], timeout=timeout)          # прогрев (вне замера)
    times = []
    for _ in range(n):
        t = time.perf_counter()
        r = s.get(spec["rest"], timeout=timeout)
        times.append((time.perf_counter() - t) * 1000.0)
        if r.status_code >= 400:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:80]}")
        if pace:
            time.sleep(pace)
    return _stat(times)


def measure_upgrade(spec, n, timeout):
    """WS-upgrade: n раз открыть и закрыть сокет (min≈прогретый, без cold DNS)."""
    if not WS_OK:
        raise RuntimeError("нет websocket-client")
    pace = spec.get("pace", 0)
    times = []
    for i in range(n):
        t = time.perf_counter()
        ws = create_connection(spec["ws"], timeout=timeout,
                               header=[f"User-Agent: {UA}"])
        times.append((time.perf_counter() - t) * 1000.0)
        ws.close()
        if pace:
            time.sleep(pace)
    return _stat(times)


def _recv_until(ws, pred, spec, deadline):
    """Читаем кадры до выполнения pred(obj, text, kind) или дедлайна.

    Управляющие PING сервера отвечаем PONG; app-level "Ping" (BingX) — "Pong",
    чтобы соединение не закрылось во время замера."""
    while time.perf_counter() < deadline:
        ws.settimeout(max(0.05, min(1.0, deadline - time.perf_counter())))
        try:
            fr = ws.recv_frame()
        except WebSocketTimeoutException:
            continue
        if fr is None:
            continue
        op = fr.opcode
        if op == ABNF.OPCODE_PING:
            try:
                ws.pong(fr.data)
            except Exception:
                pass
            if pred(None, "", "PING"):
                return
            continue
        if op == ABNF.OPCODE_PONG:
            if pred(None, "", "PONG"):
                return
            continue
        if op == ABNF.OPCODE_CLOSE:
            raise RuntimeError("сервер закрыл соединение")
        if op not in (ABNF.OPCODE_TEXT, ABNF.OPCODE_BINARY):
            continue
        data = fr.data
        if spec.get("gzip"):
            try:
                data = gzip.decompress(data)
            except Exception:
                pass
        try:
            text = data.decode("utf-8", "replace")
        except Exception:
            text = ""
        if text.strip() == "Ping":                # BingX keep-alive
            try:
                ws.send("Pong")
            except Exception:
                pass
            continue
        obj = _json(text)
        if pred(obj, text, "DATA"):
            return
    raise TimeoutError("нет ожидаемого ответа за отведённое время")


def measure_subscribe(spec, n, timeout):
    """Подписка: сокет открыт ВНЕ таймера, меряем только subscribe→ack.
    Одиночный сбой соединения не рушит весь замер — берём успешные попытки."""
    if not WS_OK:
        raise RuntimeError("нет websocket-client")
    ack = spec["ack"]
    times = []
    last_err = None
    for _ in range(n):
        try:
            ws = create_connection(spec["ws"], timeout=timeout,
                                   header=[f"User-Agent: {UA}"])
        except Exception as e:
            last_err = e
            continue
        try:
            t = time.perf_counter()
            ws.send(spec["sub"])
            _recv_until(ws, lambda o, x, k: k == "DATA" and ack(o, x),
                        spec, time.perf_counter() + timeout)
            times.append((time.perf_counter() - t) * 1000.0)
        except Exception as e:
            last_err = e
        finally:
            try:
                ws.close()
            except Exception:
                pass
        if spec.get("pace"):
            time.sleep(spec["pace"])
    if not times:
        raise last_err or RuntimeError("подписка не удалась")
    return _stat(times)


def measure_ping(spec, n, timeout):
    """Ping/pong RTT на прогретом сокете. App-level ping, где есть; иначе —
    управляющий WS-ping (RFC6455)."""
    if not WS_OK:
        raise RuntimeError("нет websocket-client")
    # Пингуем БЕЗ подписки: иначе поток данных канала копится в сокете и «прогрёб»
    # этого бэклога раздувает замер pong (заметно на чатных каналах, напр. Upbit).
    ws = create_connection(spec["ws"], timeout=timeout,
                           header=[f"User-Agent: {UA}"])
    try:
        app_ping = spec.get("ping")
        pong = spec.get("pong")
        pace = spec.get("pace") or 0.03                 # мягкий интервал (вне таймера)
        times = []
        last_err = None
        for i in range(n):
            deadline = time.perf_counter() + timeout
            t = time.perf_counter()
            try:
                if app_ping is not None:
                    ws.send(app_ping)
                    _recv_until(ws, lambda o, x, k: k == "DATA" and pong(o, x),
                                spec, deadline)
                else:
                    ws.ping(b"lt")
                    _recv_until(ws, lambda o, x, k: k == "PONG", spec, deadline)
                times.append((time.perf_counter() - t) * 1000.0)
            except Exception as e:                      # напр. сервер закрыл сокет на спам
                last_err = e
                break
            time.sleep(pace)
        if not times:
            raise last_err or RuntimeError("ping не удался")
        return _stat(times)
    finally:
        try:
            ws.close()
        except Exception:
            pass


# =========================================================================== #
#                              Вывод                                          #
# =========================================================================== #
def print_resolve(specs):
    print("\nРезолв адресов бирж (IP → reverse-DNS → регион/CDN)")
    print("─" * 92)
    seen = {}
    for spec in specs:
        host, _ = _host_port(spec["ws"])
        if host in seen:
            ip, rdns, hint = seen[host]
        else:
            try:
                ip, rdns = _resolve(host)
                hint = _region_hint(ip, rdns)
            except Exception as e:
                ip, rdns, hint = "—", str(e)[:40], "?"
            seen[host] = (ip, rdns, hint)
        rdns_disp = (rdns or "-")
        if len(rdns_disp) > 42:
            rdns_disp = rdns_disp[:41] + "…"
        print(f"{spec['name']:<24}{host:<34}")
        print(f"{'':<4}{ip:<17}{rdns_disp:<44}{hint}")
    print("─" * 92)
    print("Подсказка региона по rDNS: ec2-*.compute-1 = us-east-1; "
          "ec2-*.<region>.compute = этот регион.")
    print("CDN-edge (CloudFront/Cloudflare/Akamai) — это НЕ матчинг-движок, "
          "а ближайшая точка сети CDN.")


def print_breakdown(specs, timeout):
    print("\nРазбивка соединения по WS-хосту, мс (DNS / TCP / TLS), мин из 3")
    print("─" * 72)
    print(f"{'':<24}{'DNS':>10}{'TCP':>10}{'TLS':>10}")
    print("─" * 72)
    for spec in specs:
        host, port = _host_port(spec["ws"])
        try:
            dns, tcp, tls = _conn_breakdown(host, port, timeout)
            print(f"{spec['name']:<24}{dns:>10.1f}{tcp:>10.1f}{tls:>10.1f}")
        except Exception as e:
            print(f"{spec['name']:<24}  ✗ {str(e)[:40]}")
    print("─" * 72)
    print("TCP/TLS часто малы, потому что терминируются на CDN-edge, а не на "
          "движке. Смотрите Подписку/Ping.")


def _conn_breakdown(host, port, timeout):
    best = None
    for _ in range(3):
        t = time.perf_counter()
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
        af, socktype, proto, _, sa = infos[0]
        dns = (time.perf_counter() - t) * 1000.0
        s = socket.socket(af, socktype, proto)
        s.settimeout(timeout)
        t = time.perf_counter()
        s.connect(sa)
        tcp = (time.perf_counter() - t) * 1000.0
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        t = time.perf_counter()
        ss = ctx.wrap_socket(s, server_hostname=host)
        ss.do_handshake()
        tls = (time.perf_counter() - t) * 1000.0
        ss.close()
        if best is None or (dns + tcp + tls) < sum(best):
            best = (dns, tcp, tls)
    return best


def run_table(specs, reps, connects, timeout):
    print(f"\nПубличная задержка до бирж, мс (min/median). "
          f"REST×{reps}, upgrade×{connects}, subscribe×{max(3, connects)}, ping×{reps}")
    print("═" * 104)
    print(f"{'Биржа':<24}{'REST RTT':>14}{'WS-upgrade':>14}"
          f"{'Подписка':>14}{'Ping/pong':>14}   Регион/примечание")
    print("═" * 104)
    for spec in specs:
        cells = {}
        for name, fn, n in (
            ("rest", measure_rest, reps),
            ("upg", measure_upgrade, connects),
            ("sub", measure_subscribe, max(3, connects)),
            ("ping", measure_ping, reps),
        ):
            try:
                cells[name] = _fmt(fn(spec, n, timeout))
            except Exception as e:
                cells[name] = "✗"
                cells[name + "_err"] = str(e)
        host, _ = _host_port(spec["ws"])
        try:
            ip, rdns = _resolve(host)
            hint = _region_hint(ip, rdns)
        except Exception:
            hint = "?"
        note = spec.get("note") or hint
        print(f"{spec['name']:<24}{cells['rest']:>14}{cells['upg']:>14}"
              f"{cells['sub']:>14}{cells['ping']:>14}   {hint}")
        errs = [f"{k[:-4]}: {_oneline(v)}" for k, v in cells.items()
                if k.endswith("_err")]
        for e in errs:
            print(f"{'':<24}  ✗ {e}")
        if note and note != hint:
            print(f"{'':<24}  прим.: {note}")
    print("═" * 104)
    print("REST RTT / WS-upgrade часто отражают CDN-edge. Для выбора региона "
          "смотрите Подписку и Ping/pong — они идут до origin-шлюза биржи.")


def main():
    p = argparse.ArgumentParser(
        description="Публичный замер задержки до бирж (без ключей и ордеров).")
    p.add_argument("--exchange", default="all",
                   help="список ключей через запятую (bybit,mexc,...) или подстрока; "
                        "по умолчанию все. Ключи см. --list")
    p.add_argument("--market", default="both", choices=("both", "spot", "futures"))
    p.add_argument("--repeats", type=int, default=20, help="замеров REST и ping")
    p.add_argument("--connects", type=int, default=8, help="замеров WS-upgrade")
    p.add_argument("--timeout", type=float, default=6.0, help="таймаут на операцию, с")
    p.add_argument("--resolve-only", action="store_true", help="только IP/rDNS/регион")
    p.add_argument("--breakdown", action="store_true", help="+ DNS/TCP/TLS раздельно")
    p.add_argument("--list", action="store_true", help="показать ключи бирж и выйти")
    args = p.parse_args()

    if args.list:
        print("Доступные ключи (--exchange):")
        for s in SPECS:
            print(f"  {s['key']:<18}{s['name']:<26}{s['market']}")
        return

    sel = args.exchange.strip().lower()
    keys = [k.strip() for k in sel.split(",")] if sel not in ("all", "") else None
    specs = []
    for s in SPECS:
        if args.market != "both" and s["market"] != args.market:
            continue
        if keys is not None and not any(k == s["key"] or k in s["key"] for k in keys):
            continue
        specs.append(s)
    if not specs:
        print(f"Нет бирж под фильтр exchange={args.exchange} market={args.market}. "
              f"Список: --list")
        return

    if not WS_OK:
        print("⚠ нет пакета websocket-client — WS-замеры недоступны "
              "(pip install -r requirements.txt)")

    print_resolve(specs)
    if args.resolve_only:
        return
    if args.breakdown:
        print_breakdown(specs, args.timeout)
    run_table(specs, args.repeats, args.connects, args.timeout)


if __name__ == "__main__":
    main()
