def transform_fireant(items: list) -> list:
    rows = []

    for item in items:
        if not isinstance(item, dict):
            continue

        volume = item.get("totalVolume")

        # 🔥 FIX BIGINT
        if volume is not None:
            volume = int(float(volume))   # convert "0.0" → 0

        rows.append({
            "date": item.get("date"),
            "symbol": item.get("symbol"),
            "priceopen": item.get("priceOpen"),
            "pricehigh": item.get("priceHigh"),
            "pricelow": item.get("priceLow"),
            "priceclose": item.get("priceClose"),
            "totalvolume": volume,
        })

    return rows