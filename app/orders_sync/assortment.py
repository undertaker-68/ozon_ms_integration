from __future__ import annotations

from typing import Any, Dict, Optional, List


class AssortmentResolver:
    def __init__(self, ms):
        self.ms = ms
        self._cache: dict[str, dict] = {}

    def _first_row(self, path: str, flt: str) -> Optional[dict]:
        resp = self.ms.get(path, params={"filter": flt, "limit": 2})
        rows = resp.get("rows") or []
        if not rows:
            return None
        if len(rows) > 1:
            raise ValueError(f"Multiple rows for {path} filter={flt}")
        return rows[0]

    def get_by_article(self, article: str) -> dict:
        article = str(article).strip()
        if article in self._cache:
            return self._cache[article]

        # Пробуем по сущностям, где article точно существует
        entities = ["/entity/product", "/entity/bundle"]

        # В МС иногда для "числовых" артикулов лучше без кавычек
        filters = [f"article={article}", f'article="{article}"']

        found: Optional[dict] = None
        for ent in entities:
            for flt in filters:
                row = self._first_row(ent, flt)
                if row:
                    found = row
                    break
            if found:
                break

        if not found:
            raise KeyError(f"Assortment not found by article={article}")

        self._cache[article] = found
        return found


def extract_sale_price_cents(ass: dict[str, Any]) -> int:
    sale_prices = ass.get("salePrices") or []
    for sp in sale_prices:
        pt = (sp.get("priceType") or {}).get("name")
        val = (sp.get("value") or 0)
        if pt and pt.lower().strip() == "цена продажи":
            return int(val)
    if sale_prices:
        return int((sale_prices[0].get("value") or 0))
    return int(ass.get("price") or 0)
