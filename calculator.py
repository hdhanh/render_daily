# calculator.py — Tính MA5/20/50 và tín hiệu ThMA


def _calc_ma(closes: list[float], period: int) -> float:
    """Moving Average `period` ngày (closes sắp xếp cũ → mới)."""
    if len(closes) < period:
        return 0.0  # chưa đủ dữ liệu → 0
    return round(sum(closes[-period:]) / period, 4)


def _calc_thma(
    close_today: float, close_prev: float,
    ma_today: float,    ma_prev: float,
) -> str | None:
    """
    Tín hiệu ThMA:
      Up   — close > MA cả 2 ngày
      Down — close ≤ MA cả 2 ngày
      Buy  — close(hôm nay) > MA, close(hôm qua) ≤ MA
      Sell — close(hôm nay) ≤ MA, close(hôm qua) > MA
    Trả về None nếu MA chưa đủ dữ liệu (= 0).
    """
    if ma_today == 0 or ma_prev == 0:
        return None

    above_today = close_today > ma_today
    above_prev  = close_prev  > ma_prev

    if above_today and above_prev:
        return "Up"
    if not above_today and not above_prev:
        return "Down"
    if above_today and not above_prev:
        return "Buy"
    return "Sell"


def calc_indicators(history: list[dict]) -> list[dict]:
    """
    Tính MA5/20/50 + ThMA5/20/50 cho TOÀN BỘ lịch sử của 1 symbol.

    Tham số:
        history : list[{"date": str, "priceclose": float}]
                  sắp xếp tăng dần (cũ → mới).

    Trả về:
        list[{"date", "ma5", "ma20", "ma50", "thma5", "thma20", "thma50"}]
        Cùng thứ tự với history (cũ → mới).
    """
    closes  = [r["priceclose"] for r in history]
    results = []

    for i, row in enumerate(history):
        date        = row["date"][:10]
        close_today = closes[i]
        upto_today  = closes[:i + 1]

        ma5_today  = _calc_ma(upto_today, 5)
        ma20_today = _calc_ma(upto_today, 20)
        ma50_today = _calc_ma(upto_today, 50)

        if i > 0:
            close_prev = closes[i - 1]
            upto_prev  = closes[:i]

            thma5  = _calc_thma(close_today, close_prev, ma5_today,  _calc_ma(upto_prev, 5))
            thma20 = _calc_thma(close_today, close_prev, ma20_today, _calc_ma(upto_prev, 20))
            thma50 = _calc_thma(close_today, close_prev, ma50_today, _calc_ma(upto_prev, 50))
        else:
            thma5 = thma20 = thma50 = None  # dòng đầu tiên luôn NULL

        results.append({
            "date":   date,
            "ma5":    ma5_today,
            "ma20":   ma20_today,
            "ma50":   ma50_today,
            "thma5":  thma5,
            "thma20": thma20,
            "thma50": thma50,
        })

    return results
