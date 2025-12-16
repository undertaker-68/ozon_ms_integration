from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

from .http import request_json

MS_BASE = "https://api.moysklad.ru/api/remap/1.2"

@dataclass(frozen=True)
class StockRow:
    href: str        # meta.href for the assortment item (product/variant/bundle)
    article: str     # may be empty in report/stock/bystore
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

    # -------- Stock report --------
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
            href = href.split("?", 1)[0]
            if not href:
                continue

            article = (r.get("article") or "").strip()  # в отчёте обычно пусто
            stock = float(store_entry.get("stock") or 0)
            reserve = float(store_entry.get("reserve") or 0)

            available = stock - reserve
            if available < 0:
                available = 0.0

            out.append(StockRow(href=href, article=article, stock=stock, reserve=reserve, available=available))

        return out

    # -------- Entity resolution (href -> article) --------
    def _parse_entity_from_href(self, href: str) -> Tuple[str, str]:
        """
        href example: https://api.moysklad.ru/api/remap/1.2/entity/product/<id>
        returns ("product", "<id>")
        """
        p = urlparse(href).path  # /api/remap/1.2/entity/product/<id>
        parts = [x for x in p.split("/") if x]
        # find ".../entity/<type>/<id>"
        try:
            i = parts.index("entity")
            ent_type = parts[i + 1]
            ent_id = parts[i + 2]
            return ent_type, ent_id
        except Exception:
            return "", ""

    def _get_entities_by_ids(self, ent_type: str, ids: List[str]) -> List[Dict[str, Any]]:
        """
        GET /entity/<type>?filter=id=<id>;id=<id>...
        """
        if not ids:
            return []
        url = f"{MS_BASE}/entity/{ent_type}"
        # MoySklad filter uses semicolon for AND; multiple id=... works for "in" in practice
        flt = ";".join([f"id={i}" for i in ids])
        data = request_json("GET", url, headers=self.headers, params={"filter": flt, "limit": 1000})
        return data.get("rows") or []

    def resolve_articles_by_hrefs(self, hrefs: List[str]) -> Dict[str, str]:
        """
        Returns mapping: href -> article (offer_id in OZON).
        Pulls article from entity cards, because stock report doesn't include article.
        Supports product, bundle, variant. Falls back to code/externalCode if article missing.
        """
        # group ids by entity type
        by_type: Dict[str, List[str]] = {"product": [], "bundle": [], "variant": []}
        href_by_key: Dict[Tuple[str, str], str] = {}

        for href in hrefs:
            t, i = self._parse_entity_from_href(href)
            if t in by_type and i:
                by_type[t].append(i)
                href_by_key[(t, i)] = href

        out: Dict[str, str] = {}

        def pick_article(row: Dict[str, Any]) -> str:
            # prefer "article", then "code", then "externalCode"
            for k in ("article", "code", "externalCode"):
                v = (row.get(k) or "")
                if isinstance(v, str) and v.strip():
                    return v.strip()
            return ""

        # chunk to keep URL reasonable
        def chunks(lst: List[str], n: int = 100) -> List[List[str]]:
            return [lst[i:i+n] for i in range(0, len(lst), n)]

        for ent_type, ids in by_type.items():
            uniq = sorted(set(ids))
            for part in chunks(uniq, 100):
                rows = self._get_entities_by_ids(ent_type, part)
                for r in rows:
                    rid = r.get("id")
                    if not rid:
                        continue
                    href = href_by_key.get((ent_type, str(rid)))
                    if not href:
                        continue
                    art = pick_article(r)
                    if art:
                        out[href] = art

        return out

    # -------- Bundles list/detail --------
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
        return request_json(
            "GET",
            url,
            headers=self.headers,
            params={"expand": "components.assortment"},
        )

    def get_bundle_components(self, bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Надёжно получаем компоненты комплекта через meta.href: .../bundle/<id>/components
        """
        comps = bundle.get("components")
        if isinstance(comps, dict):
            meta = comps.get("meta") or {}
            href = meta.get("href")
            if href:
                data = request_json("GET", href, headers=self.headers, timeout=60)
                rows = data.get("rows") or []
                if isinstance(rows, list):
                    return rows
        return []
