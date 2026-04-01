# database.py

from supabase import create_client
from config import SUPABASE_URL, SUPABASE_KEY


class SupabaseClient:
    def __init__(self):
        self.client = create_client(SUPABASE_URL, SUPABASE_KEY)


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