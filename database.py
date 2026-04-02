# database.py — Supabase client

import os
from supabase import create_client


class SupabaseClient:
    def __init__(self):
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")

        if not url or not key:
            raise EnvironmentError("Thiếu biến môi trường SUPABASE_URL hoặc SUPABASE_KEY.")

        self.client = create_client(url, key)

    # ------------------------------------------------------------------ #
    #  Read                                                                #
    # ------------------------------------------------------------------ #

    def get_symbols(self) -> list[str]:
        """Lấy toàn bộ mã cổ phiếu từ bảng `main`."""
        res = self.client.table("main").select("symbol").execute()
        return [row["symbol"] for row in res.data]

    def get_recent_history(self, symbol: str, limit: int = 50) -> list[dict]:
        """
        Lấy `limit` phiên gần nhất của `symbol` từ bảng `data`.
        Kết quả trả về theo thứ tự tăng dần (cũ → mới) — sẵn sàng đưa vào calc_indicators().

        Trả về list[{"date": str, "priceclose": float}]
        """
        res = (
            self.client.table("data")
            .select("date, priceclose")
            .eq("symbol", symbol)
            .order("date", desc=True)
            .limit(limit)
            .execute()
        )
        # Supabase trả desc → đảo lại để cũ → mới
        return list(reversed(res.data))

    # ------------------------------------------------------------------ #
    #  Write                                                               #
    # ------------------------------------------------------------------ #

    def upsert_many(self, table_name: str, rows: list, on_conflict: str = "symbol,date"):
        """Upsert nhiều row vào `table_name`. Bỏ qua nếu list rỗng."""
        if not rows:
            return
        self.client.table(table_name).upsert(rows, on_conflict=on_conflict).execute()
