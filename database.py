# database.py

import os
from supabase import create_client


class SupabaseClient:
    def __init__(self):
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")

        if not url or not key:
            raise EnvironmentError(
                "Thiếu biến môi trường SUPABASE_URL hoặc SUPABASE_KEY."
            )

        self.client = create_client(url, key)

    def get_symbols(self) -> list:
        """Lấy toàn bộ mã cổ phiếu từ table main."""
        res = self.client.table("main").select("symbol").execute()
        return [row["symbol"] for row in res.data]

    def upsert_many(self, table_name: str, rows: list, on_conflict: str = "symbol,date"):
        if not rows:
            return
        self.client.table(table_name).upsert(
            rows,
            on_conflict=on_conflict
        ).execute()
