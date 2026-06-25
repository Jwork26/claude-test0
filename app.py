"""메인 서버 (Render가 실행하는 진입점).

Yahoo Finance v7 REST API를 직접 호출해 quote 데이터를 가져온다.
(yfinance의 내부 캐시 문제를 우회)
백그라운드 WebSocket 스레드로 BOATS 오버나이트 실시간 체결가를 보완한다.
"""
import base64
import json
import os
import struct
import threading
import time
import urllib.request
import http.cookiejar
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
REST_TTL = 20   # 20초 캐시 (더 자주 갱신)

_MH = {0: "프리마켓", 1: "장중", 2: "애프터마켓", 3: "오버나이트"}

WS_URL = "wss://streamer.finance.yahoo.com/?version=2"
VERSION = "direct-api-v2"

# ── Yahoo Finance v7 직접 호출 ────────────────────────────────────────────────

_cookie_jar = http.cookiejar.CookieJar()
_opener = urllib.request.build_opener(
    urllib.request.HTTPCookieProcessor(_cookie_jar)
)
_crumb: str = ""
_crumb_ts: float = 0.0
_crumb_lock = threading.Lock()
CRUMB_TTL = 3600

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}


def _get_crumb() -> str:
    global _crumb, _crumb_ts
    with _crumb_lock:
        if _crumb and time.time() - _crumb_ts < CRUMB_TTL:
            return _crumb
        try:
            req = urllib.request.Request("https://fc.yahoo.com", headers=_HEADERS)
            _opener.open(req, timeout=8)
            req = urllib.request.Request(
                "https://query1.finance.yahoo.com/v1/test/getcrumb",
                headers=_HEADERS,
            )
            crumb = _opener.open(req, timeout=8).read().decode().strip()
            if crumb and "Unauthorized" not in crumb:
                _crumb = crumb
                _crumb_ts = time.time()
                return _crumb
        except Exception:
            pass
        return _crumb  # 실패 시 이전 crumb 재사용


def _fetch_v7(symbol: str) -> dict | None:
    """Yahoo Finance v7/quote API 직접 호출 → result dict 반환."""
    crumb = _get_crumb()
    if not crumb:
        return None
    url = (
        f"https://query1.finance.yahoo.com/v7/finance/quote"
        f"?symbols={symbol}&crumb={crumb}"
    )
    try:
        req = urllib.request.Request(url, headers=_HEADERS)
        resp = _opener.open(req, timeout=10)
        data = json.loads(resp.read())
        results = data.get("quoteResponse", {}).get("result", [])
        return results[0] if results else None
    except Exception:
        return None


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
    """PricingData protobuf:
    field 1 = id (string, wire 2)
    field 2 = price (float32, wire 5)
    field 7 = marketHours (sint32, wire 0)
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
            if wire == 0:
                val, pos = _decode_varint(data, pos)
                if field == 7:
                    out["marketHours"] = val
            elif wire == 1:
                pos += 8
            elif wire == 2:
                length, pos = _decode_varint(data, pos)
                chunk = data[pos:pos + length]; pos += length
                if field == 1:
                    try: out["id"] = chunk.decode("utf-8")
                    except Exception: pass
            elif wire == 5:
                val = struct.unpack_from("<f", data, pos)[0]; pos += 4
                if field == 2:
                    out["price"] = val
            else:
                break
        except Exception:
            break
    return out


def _b64_decode_robust(msg_str: str) -> bytes:
    """offset 0~3을 시도해 유효한 base64 offset을 찾아 디코딩.
    length % 4 == 1인 offset은 수학적으로 불가 → 건너뜀.
    (예: 69자 → 69%4=1 skip → offset 1 → 68자 → 68%4=0 → 성공)
    """
    for offset in range(min(4, len(msg_str))):
        s = msg_str[offset:]
        if len(s) % 4 == 1:   # 불가능한 길이 → skip
            continue
        padded = s + "=" * (-len(s) % 4)
        for fn in (base64.b64decode, base64.urlsafe_b64decode):
            try:
                return fn(padded)
            except Exception:
                pass
    raise ValueError(f"base64 decode failed, len={len(msg_str)}, prefix={msg_str[:6]!r}")


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
        raw_repr = repr(message[:12]) if isinstance(message, (str, bytes)) else "?"
        try:
            # BINARY 프레임 → raw protobuf 직접 파싱
            if isinstance(message, bytes):
                msg = _parse_pricing(message)
                if msg.get("id") and msg.get("price", 0) > 0:
                    sym, price, mh = msg["id"], msg["price"], msg.get("marketHours")
                    with _live_lock:
                        _live[sym] = {"price": round(float(price), 2),
                                      "label": _MH.get(mh, ""), "ts": time.time()}
                with _live_lock:
                    _live["__last_msg__"] = {
                        "mode": "binary", "parsed": str(msg)[:120],
                        "raw_prefix": raw_repr, "ts": time.time(),
                    }
                return

            # TEXT 프레임 → base64 디코딩
            msg_str = message.strip()
            raw = _b64_decode_robust(msg_str)
            msg = _parse_pricing(raw)
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
            with _live_lock:
                _live["__last_msg__"] = {
                    "mode": "text", "raw_len": len(raw),
                    "raw_prefix": raw_repr, "parsed": str(msg)[:120],
                    "ts": time.time(),
                }
        except Exception as e:
            with _live_lock:
                _live["__msg_error__"] = {
                    "err": str(e)[:200], "raw_prefix": raw_repr,
                    "ts": time.time(),
                }

    def on_error(ws, err):
        with _live_lock:
            _live["__error__"] = {"err": str(err)[:300], "ts": time.time()}

    def on_close(ws, code, msg):
        with _live_lock:
            _live["__close__"] = {"code": code, "ts": time.time()}

    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                header={"User-Agent": "Mozilla/5.0"},
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            with _live_lock:
                _live["__exception__"] = {"err": str(e)[:300], "ts": time.time()}
        time.sleep(5)


def ensure_subscribed(symbol: str):
    with _sub_lock:
        if symbol not in _subscribed:
            _subscribed.add(symbol)


# ── REST 캐시 ─────────────────────────────────────────────────────────────────

def rest_data(symbol: str) -> dict | None:
    now = time.time()
    with _rest_lock:
        hit = _rest_cache.get(symbol)
        if hit and now - hit[0] < REST_TTL:
            return hit[1]

    # 1차: Yahoo v7 직접 호출
    q = _fetch_v7(symbol)

    # 2차 fallback: yfinance
    if q is None:
        try:
            info = yf.Ticker(symbol).info
            if not info or info.get("regularMarketPrice") is None:
                return None
            q = info
        except Exception:
            return None

    if q.get("regularMarketPrice") is None:
        return None

    # 최근 4일 종가는 yfinance history에서 가져옴
    recent = []
    try:
        closes = yf.Ticker(symbol).history(period="10d", interval="1d")["Close"].dropna()
        for dt, c in list(closes.items())[-4:]:
            recent.append({"date": dt.strftime("%m/%d"), "close": round(float(c), 2)})
    except Exception:
        pass

    data = {
        "name":     q.get("longName") or q.get("shortName") or symbol,
        "exchange": q.get("fullExchangeName") or q.get("exchange") or "",
        "marketState":        q.get("marketState", ""),
        "regularMarketPrice": q.get("regularMarketPrice"),
        "preMarketPrice":     q.get("preMarketPrice"),
        "postMarketPrice":    q.get("postMarketPrice"),
        "previousClose": q.get("regularMarketPreviousClose") or q.get("previousClose"),
        "open":    q.get("regularMarketOpen") or q.get("open"),
        "dayHigh": q.get("regularMarketDayHigh") or q.get("dayHigh"),
        "dayLow":  q.get("regularMarketDayLow") or q.get("dayLow"),
        "currency":     q.get("currency", "USD"),
        "recentCloses": recent,
    }
    with _rest_lock:
        _rest_cache[symbol] = (now, data)
    return data


def rest_fallback(d: dict):
    st = d["marketState"]
    if st == "PRE" and d.get("preMarketPrice"):
        return d["preMarketPrice"], "프리마켓"
    # PREPRE = 오버나이트(8PM-4AM ET): Yahoo REST에서도 postMarketPrice에 저장
    if st in ("POST", "POSTPOST", "PREPRE") and d.get("postMarketPrice"):
        label = "오버나이트" if st == "PREPRE" else "애프터마켓"
        return d["postMarketPrice"], label
    if st == "REGULAR":
        return d["regularMarketPrice"], "장중"
    return d["regularMarketPrice"], "장마감(종가)"


# ── Flask 라우트 ──────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(".", "index.html")


@app.route("/api/version")
def version_route():
    crumb_ok = bool(_crumb)
    return jsonify({"version": VERSION, "crumb_ok": crumb_ok})


@app.route("/api/ws-status")
def ws_status():
    with _live_lock:
        live_copy = dict(_live)
    price_cache = {k: {"price": v["price"], "label": v["label"],
                       "age_sec": round(time.time() - v["ts"])}
                   for k, v in live_copy.items() if not k.startswith("__")}
    debug_info  = {k: v for k, v in live_copy.items() if k.startswith("__")}
    with _sub_lock:
        subs = list(_subscribed)
    return jsonify({"version": VERSION, "subscribed": subs,
                    "price_cache": price_cache, "count": len(price_cache),
                    "debug": debug_info})


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

    prev      = d["previousClose"]
    reg_price = d["regularMarketPrice"]
    change     = round(price - prev, 4) if price and prev else None
    change_pct = round(change / prev * 100, 4) if change and prev else None

    st = d["marketState"]
    is_postmarket  = st in ("POST", "POSTPOST", "PREPRE")
    reg_change     = round(reg_price - prev, 4) if reg_price and prev else None
    reg_change_pct = round(reg_change / prev * 100, 4) if reg_change and prev else None

    now     = datetime.now(tz=ET)
    now_kst = now.astimezone(KST)
    return jsonify({
        "symbol":           symbol,
        "name":             d["name"],
        "exchange":         d.get("exchange", ""),
        "marketLabel":      label,
        "marketState":      st,
        "isPostMarket":     is_postmarket,
        "source":           source,
        "price":            price,
        "regularPrice":     reg_price,
        "regularChange":    reg_change,
        "regularChangePct": reg_change_pct,
        "previousClose":    prev,
        "open":             d["open"],
        "dayHigh":          d["dayHigh"],
        "dayLow":           d["dayLow"],
        "currency":         d["currency"],
        "change":           change,
        "changePct":        change_pct,
        "recentCloses":     d["recentCloses"],
        "fetchedKST":       now_kst.strftime("%Y-%m-%d %H:%M:%S"),
        "fetchedET":        now.strftime("%I:%M:%S %p %Z"),
    })


threading.Thread(target=_run_ws, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
