from _future_ import annotations

from typing import Any, Dict, List
import math

def _id_from_href(href: str) -> str:
    if not href:
        return ""
    return href.rstrip("/").split("/")[-1]

def availability_by_href(stock_rows) -> Dict[str, float]:
    """
    Строим словарь доступности:
    - по полному href
    - и по id (последний сегмент URL)
    """
    avail: Dict[str, float] = {}
    for r in stock_rows:
        a = float(r.available or 0.0)
        if r.href:
            avail[r.href] = a
            avail[_id_from_href(r.href)] = a
    return avail

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
        cid = _id_from_href(href)

        avail = avail_by_href.get(href)
        if avail is None:
            avail = avail_by_href.get(cid, 0.0)

        mins.append(float(avail) / qty)

    if not mins:
        return 0

    v = math.floor(min(mins))
    return int(max(0, v))
EOF
