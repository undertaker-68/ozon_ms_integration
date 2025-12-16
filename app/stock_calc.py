from __future__ import annotations

from typing import Any, Dict, List
import math

def availability_by_href(stock_rows) -> Dict[str, float]:
    return {r.href: float(r.available) for r in stock_rows}

def compute_bundle_stock(bundle: Dict[str, Any], avail_by_href: Dict[str, float]) -> int:
    comps = ((bundle.get("components") or {}).get("rows")) or []
    if not comps:
        return 0

    mins: List[float] = []
    for c in comps:
        qty = float(c.get("quantity") or 0)
        if qty <= 0:
            continue
        href = (((c.get("assortment") or {}).get("meta") or {}).get("href")) or ""
        avail = float(avail_by_href.get(href, 0.0))
        mins.append(avail / qty)

    if not mins:
        return 0

    v = math.floor(min(mins))
    return int(max(0, v))
