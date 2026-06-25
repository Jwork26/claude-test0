# 미국 주식 시세 (yfinance + Vercel)

종목명을 입력하면 장 상태(프리마켓/장중/애프터마켓)에 맞는 현재가와
전일종가·시가·고가·저가를 보여주는 웹앱. 데이터를 가져온 한국시간(KST)과
미국 동부시간(ET)을 함께 표시한다.

## 구조

- `index.html` — 프론트엔드 (정적). `/api/quote` 를 fetch, 10초 자동 갱신
- `api/quote.py` — Vercel Python 서버리스 함수. yfinance 조회 → JSON
- `requirements.txt` — yfinance, tzdata
- `app.py` — (선택) 로컬 테스트용 Flask 서버. 배포에는 불필요

## Vercel 배포 (GitHub 연동)

1. 이 폴더를 GitHub 저장소에 push
   ```bash
   git init
   git add .
   git commit -m "stock quote app"
   git branch -M main
   git remote add origin https://github.com/<사용자>/<저장소>.git
   git push -u origin main
   ```
2. https://vercel.com 가입 → **Add New → Project** → 위 GitHub 저장소 Import
3. 설정 그대로 **Deploy** (프레임워크 자동감지, 빌드 설정 불필요)
4. 발급된 `https://<프로젝트>.vercel.app` 접속 → 종목명 입력

> 이후 GitHub에 push할 때마다 Vercel이 자동 재배포한다.
> 로컬·Vercel 어디에도 직접 Python을 "설치"할 필요 없음 — Vercel이 빌드 시 처리.

## 로컬 테스트 (선택, Python 필요)

```bash
pip install flask yfinance tzdata
python app.py        # http://127.0.0.1:5000
```
