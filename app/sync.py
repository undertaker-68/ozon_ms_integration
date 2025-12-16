from __future__ import annotations

import os
import logging
from typing import Dict, Any, List

from .config import load_config
from .log import setup_logging, log_json
from .moysklad_client import MoySkladClient
from .ozon_client import OzonClient, OzonCreds
from .stock_calc import availability_by_href, compute_bundle_stock

def chunked(seq: List[Dict[str, Any]], n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def main() -> int:
    cfg = load_config()
    setup_logging(cfg.log_level)
    logger = logging.getLogger("sync")
    os.makedirs(cfg.cache_dir, exist_ok=True)

    ms = MoySkladClient(cfg.moysklad_token)

    oz1 = OzonClient(OzonCreds("OZON1", cfg.ozon1_client_id, cfg.ozon1_api_key, cfg.ozon1_warehouse_id), cfg.cache_dir)
    oz2 = OzonClient(OzonCreds("OZON2", cfg.ozon2_client_id, cfg.ozon2_api_key, cfg.ozon2_warehouse_id), cfg.cache_dir)

    # 1) Загружаем offer_id из Ozon (для маршрутизации) — отдельно по кабинетам
    oz1_ids = set()
    oz2_ids = set()

    try:
        oz1_ids = oz1.list_offer_ids()
        log_json(logger, "ozon_offer_ids_loaded", cabinet="OZON1", count=len(oz1_ids))
    except Exception as e:
        log_json(logger, "ozon_offer_ids_failed", cabinet="OZON1", error=str(e))

    try:
        oz2_ids = oz2.list_offer_ids()
        log_json(logger, "ozon_offer_ids_loaded", cabinet="OZON2", count=len(oz2_ids))
    except Exception as e:
        log_json(logger, "ozon_offer_ids_failed", cabinet="OZON2", error=str(e))

    if not oz1_ids and not oz2_ids:
        # оба кабинета недоступны — продолжать бессмысленно
        return 2

    # 2) Остатки МойСклад по складу
    try:
        report = ms.get_stock_bystore()
        rows = ms.extract_store_rows(report, cfg.moysklad_store_id)
        log_json(logger, "moysklad_stock_loaded", rows=len(rows))
    except Exception as e:
        log_json(logger, "moysklad_stock_failed", error=str(e))
        return 3

    avail_by_href = availability_by_href(rows)

    # 3) Резолвим offer_id (article) по meta.href через карточки товаров
    hrefs = [r.href for r in rows]
    href_to_article = ms.resolve_articles_by_hrefs(hrefs)

    items: List[Dict[str, Any]] = []
    for r in rows:
        art = (href_to_article.get(r.href) or "").strip()
        if not art:
            # если у товара нет артикула — просто пропускаем (можно логировать отдельно)
            continue
        items.append({"offer_id": art, "stock": int(r.available), "kind": "product"})

    # 4) Комплекты (bundle)
    try:
        bundles = ms.get_all_bundles_basic()
        log_json(logger, "moysklad_bundles_loaded", bundles=len(bundles))
        for b in bundles:
            bid = b.get("id")
            article = (b.get("article") or "").strip()
            if not bid or not article:
                continue
            # вычисляем только если есть в каком-то кабинете
            if (article not in oz1_ids) and (article not in oz2_ids):
                continue
            full = ms.get_bundle(str(bid))
            stock_val = compute_bundle_stock(full, avail_by_href)
            items.append({"offer_id": article, "stock": int(stock_val), "kind": "bundle"})
    except Exception as e:
        log_json(logger, "moysklad_bundles_failed", error=str(e))

        # 5) Маршрутизация по кабинетам
    oz1_payload: List[Dict[str, Any]] = []
    oz2_payload: List[Dict[str, Any]] = []
    missing = 0

    # Нормализация "похожих" кириллических букв -> латиница
    conf = str.maketrans({
        "А":"A","В":"B","Е":"E","К":"K","М":"M","Н":"H","О":"O","Р":"P","С":"C","Т":"T","Х":"X","У":"Y",
        "а":"a","в":"b","е":"e","к":"k","м":"m","н":"h","о":"o","р":"p","с":"c","т":"t","х":"x","у":"y",
    })

    def norm(s: str) -> str:
        return (s or "").strip().translate(conf)

    # Мапы: нормализованный offer_id -> реальный offer_id Ozon
    oz1_norm = {norm(x): x for x in oz1_ids}
    oz2_norm = {norm(x): x for x in oz2_ids}

    for it in items:
        ms_oid = it["offer_id"]
        key = norm(ms_oid)

        real1 = oz1_norm.get(key)
        real2 = oz2_norm.get(key)

        if real1:
            oz1_payload.append({"offer_id": real1, "stock": it["stock"]})
        elif real2:
            oz2_payload.append({"offer_id": real2, "stock": it["stock"]})
        else:
            missing += 1
            log_json(logger, "not_in_ozon", offer_id=ms_oid, kind=it.get("kind"))

    log_json(logger, "routing_done", ozon1=len(oz1_payload), ozon2=len(oz2_payload), missing=missing)


    # 6) Отправка остатков батчами
    def push(client: OzonClient, payload: List[Dict[str, Any]], name: str):
        if not payload:
            return
        for part in chunked(payload, 100):
            try:
                resp = client.set_stocks(part)
                log_json(logger, "ozon_stocks_sent", cabinet=name, count=len(part), response=resp)
            except Exception as e:
                log_json(logger, "ozon_stocks_failed", cabinet=name, count=len(part), error=str(e))

    push(oz1, oz1_payload, "OZON1")
    push(oz2, oz2_payload, "OZON2")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
