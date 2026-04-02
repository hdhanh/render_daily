# fetcher.py — Lấy dữ liệu lịch sử giá từ FireAnt

import requests
import time
import random

_BASE_URL = "https://restv2.fireant.vn/symbols/{symbol}/historical-quotes"

_HEADERS = {
    "accept":          "application/json, text/plain, */*",
    "accept-language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "authorization":   "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoxODg5NjIyNTMwLCJuYmYiOjE1ODk2MjI1MzAsImNsaWVudF9pZCI6ImZpcmVhbnQudHJhZGVzdGF0aW9uIiwic2NvcGUiOlsiYWNhZGVteS1yZWFkIiwiYWNhZGVteS13cml0ZSIsImFjY291bnRzLXJlYWQiLCJhY2NvdW50cy13cml0ZSIsImJsb2ctcmVhZCIsImNvbXBhbmllcy1yZWFkIiwiZmluYW5jZS1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImludmVzdG9wZWRpYS1yZWFkIiwib3JkZXJzLXJlYWQiLCJvcmRlcnMtd3JpdGUiLCJwb3N0cy1yZWFkIiwicG9zdHMtd3JpdGUiLCJzZWFyY2giLCJzeW1ib2xzLXJlYWQiLCJ1c2VyLWRhdGEtcmVhZCIsInVzZXItZGF0YS13cml0ZSIsInVzZXJzLXJlYWQiXSwianRpIjoiMjYxYTZhYWQ2MTQ5Njk1ZmJiYzcwODM5MjM0Njc1NWQifQ.dA5-HVzWv-BRfEiAd24uNBiBxASO-PAyWeWESovZm_hj4aXMAZA1-bWNZeXt88dqogo18AwpDQ-h6gefLPdZSFrG5umC1dVWaeYvUnGm62g4XS29fj6p01dhKNNqrsu5KrhnhdnKYVv9VdmbmqDfWR8wDgglk5cJFqalzq6dJWJInFQEPmUs9BW_Zs8tQDn-i5r4tYq2U8vCdqptXoM7YgPllXaPVDeccC9QNu2Xlp9WUvoROzoQXg25lFub1IYkTrM66gJ6t9fJRZToewCt495WNEOQFa_rwLCZ1QwzvL0iYkONHS_jZ0BOhBCdW9dWSawD6iF1SIQaFROvMDH1rg",
    "origin":          "https://fireant.vn",
    "referer":         "https://fireant.vn/",
    "user-agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
}

_RETRYABLE   = {429, 500, 502, 503, 504}
_MAX_RETRIES = 5
_BASE_WAIT   = 2.0   # giây, nhân đôi mỗi lần retry
_MAX_WAIT    = 60.0


def _sleep_between_pages():
    time.sleep(random.uniform(0.8, 2.0))


def _fetch_page(symbol: str, url: str, offset: int) -> list:
    """Fetch 1 page với exponential-backoff retry."""
    last_error = None

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            res = requests.get(url, headers=_HEADERS, timeout=15)

            if res.status_code == 200:
                return res.json()

            if res.status_code in _RETRYABLE:
                last_error = f"HTTP {res.status_code}"
                wait = min(_BASE_WAIT * (2 ** (attempt - 1)), _MAX_WAIT) + random.uniform(0, 1)
                print(f"   ⚠️  {symbol} offset={offset} ({last_error}), retry {attempt}/{_MAX_RETRIES} sau {wait:.1f}s...")
                time.sleep(wait)
                continue

            raise Exception(f"HTTP {res.status_code} không retry được — {symbol} offset={offset}")

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last_error = str(e)
            wait = min(_BASE_WAIT * (2 ** (attempt - 1)), _MAX_WAIT) + random.uniform(0, 1)
            print(f"   ⚠️  {symbol} offset={offset} lỗi kết nối, retry {attempt}/{_MAX_RETRIES} sau {wait:.1f}s...")
            time.sleep(wait)

    raise Exception(f"Hết {_MAX_RETRIES} lần retry — {symbol} offset={offset} — lỗi: {last_error}")


def fetch_quotes(symbol: str, start_date: str, end_date: str) -> list:
    """
    Lấy toàn bộ dữ liệu giá của `symbol` trong khoảng [start_date, end_date].
    Tự động phân trang (offset) cho đến khi hết dữ liệu.

    Trả về: list dict raw từ FireAnt API.
    """
    all_items = []
    offset    = 0
    limit     = 200

    while True:
        url = (
            _BASE_URL.format(symbol=symbol)
            + f"?startDate={start_date}&endDate={end_date}"
            + f"&offset={offset}&limit={limit}"
        )

        data = _fetch_page(symbol, url, offset)

        if not data:
            break

        all_items.extend(data)
        print(f"   ↳ offset {offset}: {len(data)} rows")

        if len(data) < limit:
            break  # trang cuối

        offset += limit
        _sleep_between_pages()

    return all_items
