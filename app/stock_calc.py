from __future__ import annotations

from typing import Any, Dict, List
import math
from urllib.parse import urlparse


def _norm_href(href: str) -> str:
    """
    Убираем query/fragment, приводим к виду .../entity/<type>/<id>
    """
    if not href:
        return ""
    p = urlparse(href)
    # оставляем scheme+netloc+path (без ?query)
    return f"{p.scheme}://{p.netloc}{p.path}".rstrip("/")


def _id_from_href(href: str) -> str:
    """
    Берём UUID/ID из path URL (без query).
    """
    h = _norm_href(href)
    if not h:
        return ""
    return h.rstrip("/").split("/")[-1]


def availability_by_href(stock_rows) -> Dict[str, float]:
    """
    Словарь доступности:
    - по нормализованному href
    - и по id (последний сегмент path)
    """
    avail: Dict[str, float] = {}
    for r in stock_rows:
        a = float(r.available or 0.0)
        if not r.href:
            continue
        nh = _norm_href(r.href)
        if nh:
            avail[nh] = a
            avail[_id_from_href(nh)] = a
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
        nh = _norm_href(href)
        cid = _id_from_href(nh)

        a = None
        if nh:
            a = avail_by_href.get(nh)
        if a is None and cid:
            a = avail_by_href.get(cid, 0.0)
        if a is None:
            a = 0.0

        mins.append(float(a) / qty)

    if not mins:
        return 0

    v = math.floor(min(mins))
    return int(max(0, v))
