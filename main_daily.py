# main_daily.py
# Chạy hàng ngày qua Render Cron Job
# Lấy dữ liệu giao dịch của ngày hôm nay (hoặc hôm qua nếu chưa có)

import time
import random
from datetime import datetime, timedelta
from fetcher_fireant import fetch_fireant_all
from transformer_fireant import transform_fireant
from database import SupabaseClient




def get_date_range():
    """
    Lấy từ hôm qua đến hôm nay để đảm bảo không bỏ sót.
    FireAnt upsert theo (symbol, date) nên không lo duplicate.
    """
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    return str(yesterday), str(today)


def process_symbol(symbol, db, start_date, end_date):
    try:
        items = fetch_fireant_all(symbol, start_date, end_date)
        rows = transform_fireant(items)
        if rows:
            db.upsert_many("data", rows)
        return (symbol, len(rows), None)
    except Exception as e:
        return (symbol, 0, str(e))


def main():
    db = SupabaseClient()
    symbols = db.get_symbols()
    start_date, end_date = get_date_range()

    total = len(symbols)
    ok, err = [], []

    run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{'='*50}")
    print(f"🚀 Cron Job bắt đầu: {run_time}")
    print(f"📅 Lấy dữ liệu: {start_date} → {end_date}")
    print(f"📋 Tổng symbols: {total}")
    print(f"{'='*50}")

    for i, symbol in enumerate(symbols, 1):
        sym, count, error = process_symbol(symbol, db, start_date, end_date)

        if error:
            err.append((sym, error))
            print(f"[{i}/{total}] ❌ {sym} → {error}")
        else:
            ok.append(sym)
            # Chỉ in nếu có dữ liệu mới (cuối tuần/nghỉ lễ sẽ = 0 rows)
            status = f"{count} rows" if count > 0 else "không có dữ liệu (nghỉ lễ?)"
            print(f"[{i}/{total}] ✅ {sym} → {status}")

        if i < total:
            time.sleep(random.uniform(1.0, 2.5))

    print(f"\n{'='*50}")
    print(f"✅ Thành công : {len(ok)}/{total}")
    print(f"❌ Lỗi       : {len(err)}/{total}")

    if err:
        print("\n--- Symbols lỗi ---")
        for sym, reason in err:
            print(f"  {sym}: {reason}")

    # Exit code 1 nếu có lỗi → Render sẽ đánh dấu job failed
    if err:
        exit(1)


if __name__ == "__main__":
    main()
