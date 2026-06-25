# 미국 주식 시세 (yfinance + Vercel)

종목명을 입력하면 장 상태(프리마켓/장중/애프터마켓)에 맞는 현재가와
전일종가·시가·고가·저가를 보여주는 웹앱. 데이터를 가져온 한국시간(KST)과
미국 동부시간(ET)을 함께 표시한다.

## 구조

- `index.html` — 프론트엔드 (정적). `/api/quote` 를 fetch, 10초 자동 갱신
- `server.py` — **상시 실행 서버 (Render/Railway용)**. 백그라운드 웹소켓이
  Yahoo 실시간 스트림에 연결해 최신가를 캐시 → 오버나이트(Blue Ocean ATS,
  8PM~4AM ET) 세션까지 Yahoo 웹페이지와 동일한 값 표시. OHLC/전일종가/최근
  종가는 REST로 가져와 30초 캐시.
- `Procfile`, `render.yaml` — 배포 설정 (gunicorn, **workers=1 필수**)
- `requirements.txt` — flask, gunicorn, yfinance, tzdata, websockets, protobuf
- `api/quote.py` — (구) Vercel 서버리스용. **오버나이트 미지원** (REST만). 참고용
- `app.py` — (선택) 로컬 REST-only 테스트용

> ⚠️ Vercel 서버리스는 상시 웹소켓 연결을 유지할 수 없어 오버나이트가 안 된다.
> 실시간(오버나이트 포함)을 원하면 아래 Render 방식으로 배포한다.

## Render 배포 (GitHub 연동, 실시간/오버나이트 지원)

1. GitHub에 push (이미 origin 연결됨: `git push origin main`)
2. https://render.com 가입 → **New → Web Service** → GitHub 저장소 연결
3. `render.yaml` 자동 감지 (또는 Start Command:
   `gunicorn server:app --workers 1 --threads 8 --timeout 120`)
4. **Create Web Service** → 발급된 `https://<이름>.onrender.com` 접속

> 무료 플랜은 일정 시간 미사용 시 슬립 → 첫 접속이 느릴 수 있음.
> 웹소켓 캐시 공유를 위해 **workers는 반드시 1개**로 둔다.

## 로컬 실행 (Python 필요)

```bash
pip install -r requirements.txt
python server.py     # http://127.0.0.1:5000  (웹소켓 실시간 포함)
# 또는 REST-only:  python app.py
```
