"""상시 실행 서버 (Render/Railway 등).

백그라운드 웹소켓(yfinance)이 Yahoo 실시간 스트림에 연결해 최신 체결가를
캐시한다. 이 스트림은 오버나이트(Blue Ocean ATS, 8PM~4AM ET) 세션 가격까지
포함하므로 Yahoo Finance 웹페이지와 동일한 값을 표시할 수 있다.

OHLC/전일종가/최근 종가는 REST(yf.Ticker)로 가져오고 30초 캐시한다.
'현재가'는 웹소켓 실시간가가 있으면 그것을, 없으면 REST 값을 쓴다.
"""
import os
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import yfinance as yf
from flask import Flask, jsonify, request, send_from_directory

app = Flask(__name__)
KST = ZoneInfo("Asia/Seoul")
ET = ZoneInfo("America/New_York")

DEFAULT_SYMBOLS = ["SNDK", "MU"]

# 웹소켓 실시간 캐시
_live = {}                     # symbol -> {"price":.., "marketHours":.., "time":..}
_live_lock = threading.Lock()
_subscribed = set(DEFAULT_SYMBOLS)
_sub_lock = threading.Lock()
_ws = None

# REST 캐시 (info/recent) — symbol -> (timestamp, payload)
_rest_cache = {}
_rest_lock = threading.Lock()
REST_TTL = 30  # 초

# Yahoo protobuf marketHours enum -> 라벨
_MH_LABEL = {0: "프리마켓", 1: "장중", 2: "애프터마켓", 3: "애프터마켓(오버나이트)"}


def _on_message(msg):
    sym = msg.get("id")
    if not sym:
        return
    with _live_lock:
        _live[sym] = msg


def _run_ws():
    """웹소켓 연결 유지 (끊기면 재연결). listen()은 블로킹이라 별도 스레드에서 돈다."""
    global _ws
    while True:
        try:
            _ws = yf.WebSocket()
            with _sub_lock:
                syms = list(_subscribed)
            if syms:
                _ws.subscribe(syms)
            _ws.listen(_on_message)  # blocking
        except Exception:
            _ws = None
            time.sleep(3)            # 재연결 대기


def ensure_subscribed(symbol):
    """요청된 종목을 아직 구독 안 했으면 추가 구독."""
    with _sub_lock:
        if symbol in _subscribed:
            return
        _subscribed.add(symbol)
    if _ws is not None:
        try:
            _ws.subscribe([symbol])
        except Exception:
            pass


def rest_data(symbol):
    """OHLC/전일종가/최근4일종가 (30초 캐시)."""
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
        "name": info.get("longName") or info.get("shortName") or symbol,
        "marketState": info.get("marketState", ""),
        "regularMarketPrice": info.get("regularMarketPrice"),
        "preMarketPrice": info.get("preMarketPrice"),
        "postMarketPrice": info.get("postMarketPrice"),
        "previousClose": info.get("regularMarketPreviousClose") or info.get("previousClose"),
        "open": info.get("regularMarketOpen") or info.get("open"),
        "dayHigh": info.get("dayHigh") or info.get("regularMarketDayHigh"),
        "dayLow": info.get("dayLow") or info.get("regularMarketDayLow"),
        "currency": info.get("currency", "USD"),
        "recentCloses": recent,
    }
    with _rest_lock:
        _rest_cache[symbol] = (now, data)
    return data


def rest_price_label(d):
    """웹소켓 값이 없을 때 쓸 REST 기준 현재가/라벨."""
    st = d["marketState"]
    if st in ("PRE", "PREPRE") and d.get("preMarketPrice") is not None:
        return d["preMarketPrice"], "프리마켓"
    if st in ("POST", "POSTPOST") and d.get("postMarketPrice") is not None:
        return d["postMarketPrice"], "애프터마켓"
    if st == "REGULAR":
        return d["regularMarketPrice"], "장중"
    return d["regularMarketPrice"], "장마감(종가)"


@app.route("/")
def index():
    return send_from_directory(".", "index.html")


@app.route("/api/ws-status")
def ws_status():
    """웹소켓 연결 및 캐시 상태 진단용."""
    with _live_lock:
        live_snapshot = {k: {"price": v.get("price"), "marketHours": v.get("marketHours"),
                             "time": v.get("time")} for k, v in _live.items()}
    with _sub_lock:
        subs = list(_subscribed)
    return jsonify({
        "ws_connected": _ws is not None,
        "subscribed": subs,
        "live_cache": live_snapshot,
        "cache_count": len(live_snapshot),
    })


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

    # 기본은 REST, 웹소켓 실시간가가 있으면 그걸로 덮어씀(웹페이지와 동일)
    price, label = rest_price_label(d)
    source = "REST"
    with _live_lock:
        msg = _live.get(symbol)
    if msg and msg.get("price") is not None:
        price = round(float(msg["price"]), 2)
        source = "LIVE"
        mh = msg.get("marketHours")
        label = _MH_LABEL.get(mh, label)

    prev_close = d["previousClose"]
    change = change_pct = None
    if price is not None and prev_close:
        change = round(price - prev_close, 4)
        change_pct = round(change / prev_close * 100, 4)

    now = datetime.now(tz=KST)
    return jsonify({
        "symbol": symbol,
        "name": d["name"],
        "marketLabel": label,
        "source": source,
        "price": price,
        "previousClose": prev_close,
        "open": d["open"],
        "dayHigh": d["dayHigh"],
        "dayLow": d["dayLow"],
        "currency": d["currency"],
        "change": change,
        "changePct": change_pct,
        "recentCloses": d["recentCloses"],
        "fetchedKST": now.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "fetchedET": now.astimezone(ET).strftime("%Y-%m-%d %H:%M:%S %Z"),
    })


# 웹소켓 백그라운드 스레드 시작 (gunicorn import 시에도 동작)
threading.Thread(target=_run_ws, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
