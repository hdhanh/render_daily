# fetcher_fireant.py

import requests
import time
import random

BASE_URL = "https://restv2.fireant.vn/symbols/{symbol}/historical-quotes"

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoxODg5NjIyNTMwLCJuYmYiOjE1ODk2MjI1MzAsImNsaWVudF9pZCI6ImZpcmVhbnQudHJhZGVzdGF0aW9uIiwic2NvcGUiOlsiYWNhZGVteS1yZWFkIiwiYWNhZGVteS13cml0ZSIsImFjY291bnRzLXJlYWQiLCJhY2NvdW50cy13cml0ZSIsImJsb2ctcmVhZCIsImNvbXBhbmllcy1yZWFkIiwiZmluYW5jZS1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImludmVzdG9wZWRpYS1yZWFkIiwib3JkZXJzLXJlYWQiLCJvcmRlcnMtd3JpdGUiLCJwb3N0cy1yZWFkIiwicG9zdHMtd3JpdGUiLCJzZWFyY2giLCJzeW1ib2xzLXJlYWQiLCJ1c2VyLWRhdGEtcmVhZCIsInVzZXItZGF0YS13cml0ZSIsInVzZXJzLXJlYWQiXSwianRpIjoiMjYxYTZhYWQ2MTQ5Njk1ZmJiYzcwODM5MjM0Njc1NWQifQ.dA5-HVzWv-BRfEiAd24uNBiBxASO-PAyWeWESovZm_hj4aXMAZA1-bWNZeXt88dqogo18AwpDQ-h6gefLPdZSFrG5umC1dVWaeYvUnGm62g4XS29fj6p01dhKNNqrsu5KrhnhdnKYVv9VdmbmqDfWR8wDgglk5cJFqalzq6dJWJInFQEPmUs9BW_Zs8tQDn-i5r4tYq2U8vCdqptXoM7YgPllXaPVDeccC9QNu2Xlp9WUvoROzoQXg25lFub1IYkTrM66gJ6t9fJRZToewCt495WNEOQFa_rwLCZ1QwzvL0iYkONHS_jZ0BOhBCdW9dWSawD6iF1SIQaFROvMDH1rg",
    "referer": "https://fireant.vn/",
    "origin": "https://fireant.vn",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
}

# Các status code cần retry (bị chặn hoặc lỗi tạm thời)
RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

MAX_RETRIES = 5          # Số lần retry tối đa mỗi page
BASE_BACKOFF = 2.0       # Giây chờ cơ bản khi retry (nhân đôi mỗi lần)
MAX_BACKOFF = 60.0       # Giới hạn tối đa backoff


def _sleep_random():
    """Sleep ngẫu nhiên 0.8–2.0s giữa các page để tránh bị rate limit."""
    delay = random.uniform(0.8, 2.0)
    time.sleep(delay)


def _fetch_page(symbol: str, url: str, offset: int) -> list:
    """
    Fetch một page duy nhất với exponential backoff retry.
    Trả về list data nếu thành công, raise Exception nếu hết retry.
    """
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            res = requests.get(url, headers=HEADERS, timeout=15)

            if res.status_code == 200:
                data = res.json()
                return data  # ✅ Thành công

            elif res.status_code in RETRYABLE_STATUSES:
                last_error = f"HTTP {res.status_code}"
                wait = min(BASE_BACKOFF * (2 ** (attempt - 1)), MAX_BACKOFF)
                # Thêm jitter nhỏ để tránh thundering herd
                wait += random.uniform(0, 1.0)
                print(f"   ⚠️  {symbol} offset={offset} bị chặn ({last_error}), thử lại lần {attempt}/{MAX_RETRIES} sau {wait:.1f}s...")
                time.sleep(wait)

            else:
                # Lỗi không thể retry (404, 401, v.v.)
                raise Exception(f"HTTP {res.status_code} không thể retry cho {symbol} offset={offset}")

        except requests.exceptions.ConnectionError as e:
            last_error = f"ConnectionError: {e}"
            wait = min(BASE_BACKOFF * (2 ** (attempt - 1)), MAX_BACKOFF)
            wait += random.uniform(0, 1.0)
            print(f"   ⚠️  {symbol} offset={offset} mất kết nối, thử lại lần {attempt}/{MAX_RETRIES} sau {wait:.1f}s...")
            time.sleep(wait)

        except requests.exceptions.Timeout:
            last_error = "Timeout"
            wait = min(BASE_BACKOFF * (2 ** (attempt - 1)), MAX_BACKOFF)
            wait += random.uniform(0, 1.0)
            print(f"   ⚠️  {symbol} offset={offset} timeout, thử lại lần {attempt}/{MAX_RETRIES} sau {wait:.1f}s...")
            time.sleep(wait)

    raise Exception(f"Hết retry ({MAX_RETRIES} lần) cho {symbol} offset={offset} — lỗi cuối: {last_error}")


def fetch_fireant_all(symbol: str, start_date: str, end_date: str) -> list:
    all_items = []
    offset = 0
    limit = 200

    while True:
        url = (
            BASE_URL.format(symbol=symbol)
            + f"?startDate={start_date}"
            + f"&endDate={end_date}"
            + f"&offset={offset}"
            + f"&limit={limit}"
        )

        data = _fetch_page(symbol, url, offset)

        # FireAnt trả về list trực tiếp — list rỗng = hết dữ liệu
        if not data:
            break

        all_items.extend(data)
        print(f"   ↳ offset {offset}: {len(data)} rows")

        if len(data) < limit:
            # Trang cuối, không cần fetch thêm
            break

        offset += limit
        _sleep_random()  # ✅ Random sleep giữa các page

    return all_items