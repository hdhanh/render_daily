# transformer.py — Chuẩn hóa dữ liệu raw từ FireAnt sang schema Supabase


def transform(items: list) -> list:
    """
    Chuyển list dict raw của FireAnt sang list dict khớp với schema bảng `data`.

    Trả về list[{"date", "symbol", "priceopen", "pricehigh", "pricelow",
                  "priceclose", "totalvolume"}]
    """
    rows = []

    for item in items:
        if not isinstance(item, dict):
            continue

        volume = item.get("totalVolume")
        if volume is not None:
            volume = int(float(volume))  # "0.0" → 0 (tránh lỗi BIGINT Supabase)

        rows.append({
            "date":        item.get("date"),
            "symbol":      item.get("symbol"),
            "priceopen":   item.get("priceOpen"),
            "pricehigh":   item.get("priceHigh"),
            "pricelow":    item.get("priceLow"),
            "priceclose":  item.get("priceClose"),
            "totalvolume": volume,
        })

    return rows
