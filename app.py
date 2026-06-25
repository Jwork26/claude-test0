"""메인 서버 (Render가 실행하는 진입점).

백그라운드 스레드가 Yahoo Finance WebSocket에 직접 연결해
오버나이트(Blue Ocean ATS 포함) 실시간 체결가를 캐시한다.
OHLC/전일종가/최근종가는 REST(yfinance) 30초 캐시.
"""
import base64
import json
import os
import struct
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import yfinance as yf
from flask import Flask, jsonify, request, send_from_directory

app = Flask(__name__)
KST = ZoneInfo("Asia/Seoul")
ET  = ZoneInfo("America/New_York")

DEFAULT_SYMBOLS = ["SNDK", "MU"]

_live: dict = {}
_live_lock = threading.Lock()

_subscribed: set = set(DEFAULT_SYMBOLS)
_sub_lock = threading.Lock()

_rest_cache: dict = {}
_rest_lock = threading.Lock()
REST_TTL = 30

_MH = {0: "프리마켓", 1: "장중", 2: "애프터마켓", 3: "오버나이트"}

WS_URL = "wss://streamer.finance.yahoo.com/?version=2"

VERSION = "8dcf487-fix"


# ── protobuf-lite 파서 ────────────────────────────────────────────────────────

def _decode_varint(data: bytes, pos: int):
    result, shift = 0, 0
    while pos < len(data):
        b = data[pos]; pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, pos
        shift += 7
    return result, pos


def _parse_pricing(data: bytes) -> dict:
    """PricingData protobuf 파서.
    field 1 = id (string, wire 2)
    field 2 = price (float, wire 5)
    field 7 = marketHours (int32, wire 0)
    """
    out = {}
    pos = 0
    while pos < len(data):
        try:
            tag_byte, pos = _decode_varint(data, pos)
        except Exception:
            break
        field = tag_byte >> 3
        wire  = tag_byte & 0x7
        try:
            if wire == 0:          # varint
                val, pos = _decode_varint(data, pos)
                if field == 7:
                    out["marketHours"] = val
            elif wire == 1:        # 64-bit
                pos += 8
            elif wire == 2:        # length-delimited
                length, pos = _decode_varint(data, pos)
                chunk = data[pos:pos+length]; pos += length
                if field == 1:     # id = ticker symbol
                    try: out["id"] = chunk.decode("utf-8")
                    except Exception: pass
            elif wire == 5:        # 32-bit float
                val = struct.unpack_from("<f", data, pos)[0]; pos += 4
                if field == 2:     # price
                    out["price"] = val
            else:
                break
        except Exception:
            break
    return out


# ── WebSocket 스레드 ──────────────────────────────────────────────────────────

def _run_ws():
    try:
        import websocket
    except ImportError:
        return

    def on_open(ws):
        with _sub_lock:
            syms = list(_subscribed)
        ws.send(json.dumps({"subscribe": syms}))

    def on_message(ws, message):
        try:
            raw = base64.b64decode(message)
            msg = _parse_pricing(raw)
            # 디버그: 파싱 결과를 _debug에 저장
            with _live_lock:
                _live["__debug__"] = {"raw_len": len(raw), "parsed": str(msg)[:200], "ts": time.time()}
            sym   = msg.get("id")
            price = msg.get("price")
            mh    = msg.get("marketHours")
            if sym and price and price > 0:
                with _live_lock:
                    _live[sym] = {
                        "price": round(float(price), 2),
                        "label": _MH.get(mh, ""),
                        "ts":    time.time(),
                    }
        except Exception as e:
            with _live_lock:
                _live["__error__"] = {"err": str(e), "ts": time.time()}

    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                header={"User-Agent": "Mozilla/5.0"},
                on_open=on_open,
                on_message=on_message,
                on_error=lambda ws, e: None,
                on_close=lambda ws, *a: None,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception:
            pass
        time.sleep(5)


def ensure_subscribed(symbol):
    with _sub_lock:
        if symbol not in _subscribed:
            _subscribed.add(symbol)


# ── REST 캐시 ─────────────────────────────────────────────────────────────────

def rest_data(symbol: str):
    now = time.time()
    with _rest_lock:
        hit = _rest_cache.get(symbol)
        if hit and now - hit[0] < REST_TTL:
            return hit[1]

    ticker = yf.Ticker(symbol)
    info = ticker.info
    if not info or info.get("regularMarketPrice") is None:
        return None

    recent = []
    try:
        closes = ticker.history(period="10d", interval="1d")["Close"].dropna()
        for dt, c in list(closes.items())[-4:]:
            recent.append({"date": dt.strftime("%m/%d"), "close": round(float(c), 2)})
    except Exception:
        pass

    data = {
        "name":         info.get("longName") or info.get("shortName") or symbol,
        "marketState":  info.get("marketState", ""),
        "regularMarketPrice": info.get("regularMarketPrice"),
        "preMarketPrice":     info.get("preMarketPrice"),
        "postMarketPrice":    info.get("postMarketPrice"),
        "previousClose": info.get("regularMarketPreviousClose") or info.get("previousClose"),
        "open":    info.get("regularMarketOpen") or info.get("open"),
        "dayHigh": info.get("dayHigh") or info.get("regularMarketDayHigh"),
        "dayLow":  info.get("dayLow") or info.get("regularMarketDayLow"),
        "currency":     info.get("currency", "USD"),
        "recentCloses": recent,
    }
    with _rest_lock:
        _rest_cache[symbol] = (now, data)
    return data


def rest_fallback(d: dict):
    st = d["marketState"]
    if st in ("PRE", "PREPRE") and d.get("preMarketPrice"):
        return d["preMarketPrice"], "프리마켓"
    if st in ("POST", "POSTPOST") and d.get("postMarketPrice"):
        return d["postMarketPrice"], "애프터마켓"
    if st == "REGULAR":
        return d["regularMarketPrice"], "장중"
    return d["regularMarketPrice"], "장마감(종가)"


# ── Flask 라우트 ──────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(".", "index.html")


@app.route("/api/version")
def version():
    return jsonify({"version": VERSION, "ws_backend": "websocket-client"})


@app.route("/api/ws-status")
def ws_status():
    with _live_lock:
        snap = {k: {"price": v["price"], "label": v["label"],
                    "age_sec": round(time.time() - v["ts"])}
                for k, v in _live.items()}
    with _sub_lock:
        subs = list(_subscribed)
    return jsonify({"version": VERSION, "subscribed": subs,
                    "live_cache": snap, "count": len(snap)})


@app.route("/api/quote")
def quote():
    symbol = (request.args.get("symbol") or "").strip().upper()
    if not symbol:
        return jsonify({"error": "종목명을 입력하세요."}), 400

    ensure_subscribed(symbol)
    try:
        d = rest_data(symbol)
    except Exception as e:
        return jsonify({"error": f"조회 실패: {e}"}), 502
    if d is None:
        return jsonify({"error": f"'{symbol}' 종목을 찾을 수 없습니다."}), 404

    source = "REST"
    with _live_lock:
        live = _live.get(symbol)
    if live and (time.time() - live["ts"]) < 60:
        price  = live["price"]
        label  = live["label"] or rest_fallback(d)[1]
        source = "LIVE"
    else:
        price, label = rest_fallback(d)

    prev = d["previousClose"]
    change     = round(price - prev, 4) if price and prev else None
    change_pct = round(change / prev * 100, 4) if change and prev else None

    now = datetime.now(tz=KST)
    return jsonify({
        "symbol":        symbol,
        "name":          d["name"],
        "marketLabel":   label,
        "source":        source,
        "price":         price,
        "previousClose": prev,
        "open":          d["open"],
        "dayHigh":       d["dayHigh"],
        "dayLow":        d["dayLow"],
        "currency":      d["currency"],
        "change":        change,
        "changePct":     change_pct,
        "recentCloses":  d["recentCloses"],
        "fetchedKST":    now.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "fetchedET":     now.astimezone(ET).strftime("%Y-%m-%d %H:%M:%S %Z"),
    })


threading.Thread(target=_run_ws, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
