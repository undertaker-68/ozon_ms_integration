from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from .http import request_json

MS_BASE = "https://api.moysklad.ru/api/remap/1.2"

@dataclass(frozen=True)
class StockRow:
    href: str        # meta.href for the assortment item
    article: str     # SKU/article
    stock: float
    reserve: float
    available: float

class MoySkladClient:
    def __init__(self, token: str):
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json;charset=utf-8",
            "Accept-Encoding": "gzip",
        }

    def get_stock_bystore(self) -> Dict[str, Any]:
        url = f"{MS_BASE}/report/stock/bystore"
        return request_json("GET", url, headers=self.headers, params={"stockMode": "all"})

    def extract_store_rows(self, report: Dict[str, Any], store_id: str) -> List[StockRow]:
        store_marker = f"/entity/store/{store_id}"
        out: List[StockRow] = []

        for r in (report.get("rows") or []):
            sbs = r.get("stockByStore") or []
            store_entry = None
            for s in sbs:
                href = ((s.get("meta") or {}).get("href")) or ""
                if store_marker in href:
                    store_entry = s
                    break
            if not store_entry:
                continue

            href = ((r.get("meta") or {}).get("href")) or ""
            if not href:
                continue

            article = (r.get("article") or "").strip()
            stock = float(store_entry.get("stock") or 0)
            reserve = float(store_entry.get("reserve") or 0)

            available = stock - reserve
            if available < 0:
                available = 0.0

            out.append(StockRow(href=href, article=article, stock=stock, reserve=reserve, available=available))

        return out

    # ---- Bundles ----
    def list_bundles(self, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        url = f"{MS_BASE}/entity/bundle"
        return request_json("GET", url, headers=self.headers, params={"limit": limit, "offset": offset})

    def get_all_bundles_basic(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        offset = 0
        limit = 100
        while True:
            page = self.list_bundles(limit=limit, offset=offset)
            rows = page.get("rows") or []
            out.extend(rows)
            if len(rows) < limit:
                break
            offset += limit
        return out

    def get_bundle(self, bundle_id: str) -> Dict[str, Any]:
        url = f"{MS_BASE}/entity/bundle/{bundle_id}"
        return request_json("GET", url, headers=self.headers)
