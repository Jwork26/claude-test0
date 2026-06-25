from datetime import datetime
from zoneinfo import ZoneInfo

import yfinance as yf
from flask import Flask, jsonify, request, send_from_directory

app = Flask(__name__)

KST = ZoneInfo("Asia/Seoul")
ET = ZoneInfo("America/New_York")


def pick_price(info):
    """marketState에 따라 표시할 현재가와 라벨, 등락 기준을 고른다."""
    state = info.get("marketState", "")
    prev_close = info.get("regularMarketPreviousClose") or info.get("previousClose")

    if state in ("PRE", "PREPRE") and info.get("preMarketPrice") is not None:
        return info.get("preMarketPrice"), "프리마켓", "PRE"
    if state in ("POST", "POSTPOST") and info.get("postMarketPrice") is not None:
        return info.get("postMarketPrice"), "애프터마켓", "POST"
    if state == "REGULAR":
        return info.get("regularMarketPrice"), "장중", "REGULAR"
    # CLOSED 등: 정규장 종가 표시
    return info.get("regularMarketPrice"), "장마감(종가)", "CLOSED"


@app.route("/")
def index():
    return send_from_directory(".", "index.html")


@app.route("/api/quote")
def quote():
    symbol = (request.args.get("symbol") or "").strip().upper()
    if not symbol:
        return jsonify({"error": "종목명을 입력하세요."}), 400

    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
    except Exception as e:
        return jsonify({"error": f"조회 실패: {e}"}), 502

    # 유효성: 가격 정보가 전혀 없으면 잘못된 종목
    if not info or info.get("regularMarketPrice") is None:
        return jsonify({"error": f"'{symbol}' 종목을 찾을 수 없습니다."}), 404

    # 최근 4 거래일 종가
    recent = []
    try:
        closes = ticker.history(period="10d", interval="1d")["Close"].dropna()
        for dt, c in list(closes.items())[-4:]:
            recent.append({"date": dt.strftime("%m/%d"), "close": round(float(c), 2)})
    except Exception:
        pass

    price, label, _ = pick_price(info)
    prev_close = info.get("regularMarketPreviousClose") or info.get("previousClose")

    change = None
    change_pct = None
    if price is not None and prev_close:
        change = price - prev_close
        change_pct = change / prev_close * 100

    now = datetime.now(tz=KST)
    return jsonify({
        "symbol": symbol,
        "name": info.get("longName") or info.get("shortName") or symbol,
        "marketLabel": label,
        "price": price,
        "previousClose": prev_close,
        "open": info.get("regularMarketOpen") or info.get("open"),
        "dayHigh": info.get("dayHigh") or info.get("regularMarketDayHigh"),
        "dayLow": info.get("dayLow") or info.get("regularMarketDayLow"),
        "currency": info.get("currency", "USD"),
        "change": change,
        "changePct": change_pct,
        "recentCloses": recent,
        "fetchedKST": now.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "fetchedET": now.astimezone(ET).strftime("%Y-%m-%d %H:%M:%S %Z"),
    })


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
