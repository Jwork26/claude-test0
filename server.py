"""상시 실행 서버 (Render 용).

백그라운드에서 yflive 웹소켓이 Yahoo Finance 실시간 스트림에 연결해
오버나이트(Blue Ocean ATS 포함) 체결가를 캐시한다.
OHLC/전일종가/최근종가는 REST(yfinance) 30초 캐시.
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
ET  = ZoneInfo("America/New_York")

DEFAULT_SYMBOLS = ["SNDK", "MU"]

# 실시간 캐시: symbol -> {"price": float, "label": str, "ts": float}
_live: dict = {}
_live_lock = threading.Lock()

# 구독 중인 심볼
_subscribed: set = set(DEFAULT_SYMBOLS)
_sub_lock = threading.Lock()

# REST 30초 캐시: symbol -> (timestamp, data_dict)
_rest_cache: dict = {}
_rest_lock = threading.Lock()
REST_TTL = 30

# Yahoo marketHours enum → 라벨
_MH = {0: "프리마켓", 1: "장중", 2: "애프터마켓", 3: "오버나이트"}


# ── 웹소켓 스트리밍 ──────────────────────────────────────────────────────────

def _make_streamer():
    """yflive QuoteStreamer 생성. 설치 안 됐으면 None."""
    try:
        from yflive import QuoteStreamer
        qs = QuoteStreamer()
        return qs
    except ImportError:
        return None


def _on_quote(qs, quote):
    """yflive 콜백: quote 객체 → _live 캐시 업데이트."""
    sym = getattr(quote, "id", None)
    price = getattr(quote, "price", None)
    mh = getattr(quote, "marketHours", None)
    if sym and price is not None:
        label = _MH.get(mh, "")
        with _live_lock:
            _live[sym] = {"price": round(float(price), 2),
                          "label": label,
                          "ts": time.time()}


def _run_ws():
    """웹소켓 연결 유지 스레드. 끊기면 재연결."""
    while True:
        qs = _make_streamer()
        if qs is None:
            # yflive 없음 → REST 전용으로 동작
            time.sleep(3600)
            continue
        try:
            with _sub_lock:
                syms = list(_subscribed)
            qs.subscribe(syms)
            qs.on_quote = _on_quote
            qs.start(should_thread=False)   # blocking
        except Exception:
            pass
        time.sleep(3)   # 재연결 대기


def ensure_subscribed(symbol):
    with _sub_lock:
        if symbol in _subscribed:
            return
        _subscribed.add(symbol)
    # 실행 중인 QuoteStreamer가 없으므로 재시작 시 반영됨


# ── REST 데이터 ───────────────────────────────────────────────────────────────

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


def rest_fallback_price(d: dict):
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

    # 실시간 캐시 우선, 60초 이상 지난 캐시는 버림
    price, label, source = None, None, "REST"
    with _live_lock:
        live = _live.get(symbol)
    if live and (time.time() - live["ts"]) < 60:
        price  = live["price"]
        label  = live["label"] or rest_fallback_price(d)[1]
        source = "LIVE"
    else:
        price, label = rest_fallback_price(d)

    prev = d["previousClose"]
    change = round(price - prev, 4) if price and prev else None
    change_pct = round(change / prev * 100, 4) if change and prev else None

    now = datetime.now(tz=KST)
    return jsonify({
        "symbol":       symbol,
        "name":         d["name"],
        "marketLabel":  label,
        "source":       source,
        "price":        price,
        "previousClose":prev,
        "open":         d["open"],
        "dayHigh":      d["dayHigh"],
        "dayLow":       d["dayLow"],
        "currency":     d["currency"],
        "change":       change,
        "changePct":    change_pct,
        "recentCloses": d["recentCloses"],
        "fetchedKST":   now.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "fetchedET":    now.astimezone(ET).strftime("%Y-%m-%d %H:%M:%S %Z"),
    })


@app.route("/api/ws-status")
def ws_status():
    with _live_lock:
        snap = {k: {"price": v["price"], "label": v["label"],
                    "age_sec": round(time.time() - v["ts"])}
                for k, v in _live.items()}
    with _sub_lock:
        subs = list(_subscribed)
    return jsonify({
        "subscribed":  subs,
        "live_cache":  snap,
        "cache_count": len(snap),
    })


# 웹소켓 스레드 시작
threading.Thread(target=_run_ws, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
