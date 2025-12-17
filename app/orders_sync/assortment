from __future__ import annotations

from typing import Any

class AssortmentResolver:
    def __init__(self, ms):
        self.ms = ms
        self._cache: dict[str, dict] = {}

    def get_by_article(self, article: str) -> dict:
        if article in self._cache:
            return self._cache[article]
        # В МС filter обычно требует кавычки
        resp = self.ms.get("/entity/assortment", params={"filter": f'article="{article}"', "limit": 2})
        rows = resp.get("rows") or []
        if not rows:
            raise KeyError(f"Assortment not found by article={article}")
        if len(rows) > 1:
            # лучше падать, чем создать неверный заказ
            raise ValueError(f"Multiple assortments found for article={article}")
        self._cache[article] = rows[0]
        return rows[0]

def extract_sale_price_cents(ass: dict[str, Any]) -> int:
    # МС хранит цены в "копейках" (или в минимальной валютной единице)
    sale_prices = ass.get("salePrices") or []
    # пробуем найти “Цена продажи”
    for sp in sale_prices:
        pt = (sp.get("priceType") or {}).get("name")
        val = (sp.get("value") or 0)
        if pt and pt.lower().strip() == "цена продажи":
            return int(val)
    # fallback: первая доступная цена
    if sale_prices:
        return int((sale_prices[0].get("value") or 0))
    # крайний fallback
    return int(ass.get("price") or 0)
