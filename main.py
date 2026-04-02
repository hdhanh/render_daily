# main.py — Fetch dữ liệu ngày mới nhất + tính MA/ThMA rồi upsert vào Supabase
#
# Luồng xử lý cho mỗi symbol:
#   1. Fetch dữ liệu HÔM NAY từ FireAnt
#   2. Nếu không có dữ liệu (nghỉ lễ / cuối tuần) → bỏ qua
#   3. Đọc 50 phiên GẦN NHẤT từ Supabase (dữ liệu cũ)
#   4. Ghép 50 phiên cũ + 1 phiên mới → tính MA5/20/50 và ThMA5/20/50
#   5. Upsert 1 row hoàn chỉnh (giá + indicators) vào Supabase

import time
import random
from datetime import datetime

from fetcher     import fetch_quotes
from transformer import transform
from database    import SupabaseClient
from calculator  import calc_indicators


# ------------------------------------------------------------------ #
#  Helpers                                                             #
# ------------------------------------------------------------------ #

def _today() -> str:
    return str(datetime.now().date())


def _attach_indicators(row: dict, history_old: list[dict]) -> dict:
    """
    Ghép lịch sử cũ + row mới, tính indicators, trả về row đã có MA/ThMA.

    history_old : list[{"date", "priceclose"}], sắp xếp cũ → mới (tối đa 50 phiên).
    row         : dict đầy đủ của phiên hôm nay (sau transform).
    """
    # Chỉ cần date + priceclose để tính — ghép vào cuối lịch sử
    combined = history_old + [{"date": row["date"], "priceclose": row["priceclose"]}]

    results = calc_indicators(combined)
    latest  = results[-1]  # kết quả của phiên hôm nay

    return {
        **row,
        "ma5":    latest["ma5"],
        "ma20":   latest["ma20"],
        "ma50":   latest["ma50"],
        "thma5":  latest["thma5"],
        "thma20": latest["thma20"],
        "thma50": latest["thma50"],
    }


# ------------------------------------------------------------------ #
#  Per-symbol processing                                               #
# ------------------------------------------------------------------ #

def _process_symbol(symbol: str, db: SupabaseClient, today: str) -> tuple:
    """
    Trả về (symbol, n_rows_upserted, error_message | None).
    """
    # 1. Fetch hôm nay từ FireAnt
    raw   = fetch_quotes(symbol, today, today)
    rows  = transform(raw)

    if not rows:
        return (symbol, 0, None)  # không có phiên giao dịch (nghỉ lễ / cuối tuần)

    new_row = rows[0]  # chỉ 1 phiên / ngày

    # 2. Đọc 50 phiên cũ từ Supabase
    history_old = db.get_recent_history(symbol, limit=50)

    # 3. Tính indicators rồi merge vào row
    complete_row = _attach_indicators(new_row, history_old)

    # 4. Upsert
    db.upsert_many("data", [complete_row])

    return (symbol, 1, None)


# ------------------------------------------------------------------ #
#  Main                                                                #
# ------------------------------------------------------------------ #

def main():
    db    = SupabaseClient()
    today = _today()

    symbols = db.get_symbols()
    total   = len(symbols)
    ok, err = [], []

    print(f"{'='*52}")
    print(f"🚀  Bắt đầu  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📅  Ngày     : {today}")
    print(f"📋  Symbols  : {total}")
    print(f"{'='*52}")

    for i, symbol in enumerate(symbols, 1):
        try:
            sym, count, error = _process_symbol(symbol, db, today)
        except Exception as e:
            sym, count, error = symbol, 0, str(e)

        if error:
            err.append((sym, error))
            print(f"[{i:>{len(str(total))}}/{total}] ❌  {sym} → {error}")
        else:
            ok.append(sym)
            status = f"1 row (với MA/ThMA)" if count else "bỏ qua (không có phiên)"
            print(f"[{i:>{len(str(total))}}/{total}] ✅  {sym} → {status}")

        if i < total:
            time.sleep(random.uniform(1.0, 2.5))

    print(f"\n{'='*52}")
    print(f"✅  Thành công : {len(ok)}/{total}")
    print(f"❌  Lỗi        : {len(err)}/{total}")

    if err:
        print("\n--- Symbols lỗi ---")
        for sym, reason in err:
            print(f"  {sym}: {reason}")

    if err:
        exit(1)  # GitHub Actions đánh dấu job failed


if __name__ == "__main__":
    main()
