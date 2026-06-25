import json
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import yfinance as yf

KST = ZoneInfo("Asia/Seoul")
ET = ZoneInfo("America/New_York")


def pick_price(info):
    """marketState에 따라 표시할 현재가와 라벨을 고른다."""
    state = info.get("marketState", "")
    if state in ("PRE", "PREPRE") and info.get("preMarketPrice") is not None:
        return info.get("preMarketPrice"), "프리마켓"
    if state in ("POST", "POSTPOST") and info.get("postMarketPrice") is not None:
        return info.get("postMarketPrice"), "애프터마켓"
    if state == "REGULAR":
        return info.get("regularMarketPrice"), "장중"
    return info.get("regularMarketPrice"), "장마감(종가)"


def build_payload(symbol):
    info = yf.Ticker(symbol).info
    if not info or info.get("regularMarketPrice") is None:
        return None
    price, label = pick_price(info)
    prev_close = info.get("regularMarketPreviousClose") or info.get("previousClose")
    change = change_pct = None
    if price is not None and prev_close:
        change = price - prev_close
        change_pct = change / prev_close * 100
    now = datetime.now(tz=KST)
    return {
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
        "fetchedKST": now.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "fetchedET": now.astimezone(ET).strftime("%Y-%m-%d %H:%M:%S %Z"),
    }


class handler(BaseHTTPRequestHandler):
    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        qs = parse_qs(urlparse(self.path).query)
        symbol = (qs.get("symbol", [""])[0] or "").strip().upper()
        if not symbol:
            return self._send(400, {"error": "종목명을 입력하세요."})
        try:
            payload = build_payload(symbol)
        except Exception as e:
            return self._send(502, {"error": f"조회 실패: {e}"})
        if payload is None:
            return self._send(404, {"error": f"'{symbol}' 종목을 찾을 수 없습니다."})
        return self._send(200, payload)
