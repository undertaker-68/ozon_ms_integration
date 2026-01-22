#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
from typing import Dict, Iterable, List, Tuple

from app.config import load_config
from app.moysklad_client import MoySkladClient
from app.ozon_client import OzonClient, OzonCreds


def ms_list_all(ms: MoySkladClient, entity_path: str, limit: int = 1000) -> List[dict]:
    """Постранично забираем /entity/<type>."""
    out: List[dict] = []
    offset = 0
    while True:
        page = ms.get(entity_path, params={"limit": limit, "offset": offset})
        rows = page.get("rows") or []
        out.extend(rows)
        if len(rows) < limit:
            break
        offset += limit
    return out


def pick_article(row: dict) -> str:
    # prefer "article", then "code", then "externalCode"
    for k in ("article", "code", "externalCode"):
        v = row.get(k) or ""
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def make_norm():
    # как в app/sync.py — нормализация “похожих” кириллических букв -> латиница
    conf = str.maketrans(
        {
            "А": "A",
            "В": "B",
            "Е": "E",
            "К": "K",
            "М": "M",
            "Н": "H",
            "О": "O",
            "Р": "P",
            "С": "C",
            "Т": "T",
            "Х": "X",
            "У": "Y",
            "а": "a",
            "в": "b",
            "е": "e",
            "к": "k",
            "м": "m",
            "н": "h",
            "о": "o",
            "р": "p",
            "с": "c",
            "т": "t",
            "х": "x",
            "у": "y",
        }
    )

    def norm(s: str) -> str:
        return (s or "").strip().translate(conf)

    return norm


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Проверка соответствия article (МойСклад) -> offer_id (Ozon)."
    )
    ap.add_argument(
        "--include-variants",
        action="store_true",
        help="Включать /entity/variant (если используете модификации).",
    )
    ap.add_argument(
        "--csv",
        dest="csv_path",
        default="",
        help="Если указано — сохранить CSV по пути.",
    )
    ap.add_argument(
        "--no-ozon-cache",
        action="store_true",
        help="Игнорировать кэш offer_ids и перезагрузить из Ozon.",
    )
    args = ap.parse_args()

    cfg = load_config()

    ms = MoySkladClient(cfg.moysklad_token)

    oz1 = OzonClient(
        OzonCreds("OZON1", cfg.ozon1_client_id, cfg.ozon1_api_key, cfg.ozon1_warehouse_id),
        cfg.cache_dir,
    )
    oz2 = OzonClient(
        OzonCreds("OZON2", cfg.ozon2_client_id, cfg.ozon2_api_key, cfg.ozon2_warehouse_id),
        cfg.cache_dir,
    )

    ttl = 0 if args.no_ozon_cache else 7 * 60
    oz1_ids = oz1.list_offer_ids(ttl_seconds=ttl)
    oz2_ids = oz2.list_offer_ids(ttl_seconds=ttl)

    norm = make_norm()

    # norm -> real offer_id + cabinet
    oz_norm_map: Dict[str, Tuple[str, str]] = {}
    for oid in oz1_ids:
        oz_norm_map.setdefault(norm(oid), (oid, "OZON1"))
    for oid in oz2_ids:
        oz_norm_map.setdefault(norm(oid), (oid, "OZON2"))

    # МойСклад: соберём articles у продуктов/комплектов (+опционально variants)
    ms_articles: List[Tuple[str, str]] = []  # (article, entity_kind)

    for r in ms_list_all(ms, "/entity/product"):
        a = pick_article(r)
        if a:
            ms_articles.append((a, "product"))

    for r in ms_list_all(ms, "/entity/bundle"):
        a = pick_article(r)
        if a:
            ms_articles.append((a, "bundle"))

    if args.include_variants:
        for r in ms_list_all(ms, "/entity/variant"):
            a = pick_article(r)
            if a:
                ms_articles.append((a, "variant"))

    # Уникализируем и сортируем по article
    uniq = {}
    for a, kind in ms_articles:
        uniq.setdefault(a, kind)
    ms_articles_sorted = sorted(((a, uniq[a]) for a in uniq.keys()), key=lambda x: x[0])

    rows_out: List[Dict[str, str]] = []
    not_found = 0

    for article, kind in ms_articles_sorted:
        hit = oz_norm_map.get(norm(article))
        if hit:
            offer_id, cabinet = hit
            rows_out.append(
                {
                    "article": article,
                    "offer_id": offer_id,
                    "status": "OK",
                    "cabinet": cabinet,
                    "ms_kind": kind,
                }
            )
        else:
            not_found += 1
            rows_out.append(
                {
                    "article": article,
                    "offer_id": "NOT FOUND",
                    "status": "NOT_FOUND",
                    "cabinet": "",
                    "ms_kind": kind,
                }
            )

    # Печать в stdout: именно "article -> offer_id"
    for r in rows_out:
        print(f'{r["article"]} -> {r["offer_id"]}')

    # CSV (по желанию)
    if args.csv_path:
        os.makedirs(os.path.dirname(args.csv_path) or ".", exist_ok=True)
        with open(args.csv_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(
                f, fieldnames=["article", "offer_id", "status", "cabinet", "ms_kind"]
            )
            w.writeheader()
            w.writerows(rows_out)

    print(f"\nTOTAL: {len(rows_out)} | NOT FOUND: {not_found}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
