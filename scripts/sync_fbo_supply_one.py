import os
from typing import Any, Dict, List, Optional

from app.config import load_config
from app.http import request_json
from app.moysklad_client import MoySkladClient

OZON_API = "https://api-seller.ozon.ru"
MS_BASE = "https://api.moysklad.ru/api/remap/1.2"


def ozon_headers(cfg, cabinet: int) -> Dict[str, str]:
    if cabinet == 2:
        cid, key = cfg.ozon2_client_id, cfg.ozon2_api_key
    else:
        cid, key = cfg.ozon1_client_id, cfg.ozon1_api_key

    return {
        "Client-Id": str(cid),
        "Api-Key": key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def ozon_supply_get(headers: Dict[str, str], order_id: int) -> Dict[str, Any]:
    # v3: order_ids[]
    return request_json(
        "POST",
        f"{OZON_API}/v3/supply-order/get",
        headers=headers,
        json={"order_ids": [order_id]},
    )


def ozon_bundle_items(headers: Dict[str, str], bundle_id: str) -> List[Dict[str, Any]]:
    rep = request_json(
        "POST",
        f"{OZON_API}/v1/supply-order/bundle",
        headers=headers,
        json={"bundle_ids": [bundle_id], "is_asc": True, "limit": 100},
    )
    return rep.get("items") or []


def ms_get_first_organization(ms: MoySkladClient) -> Optional[str]:
    rep = ms.request("GET", "/entity/organization?limit=100")
    rows = rep.get("rows") or []
    if not rows:
        return None
    href = (rows[0].get("meta") or {}).get("href") or ""
    return href.rsplit("/", 1)[-1] if href else None


def ms_find_counterparty_id(ms: MoySkladClient, name: str) -> Optional[str]:
    rep = ms.request("GET", f"/entity/counterparty?search={name}&limit=100")
    rows = rep.get("rows") or []
    for r in rows:
        if (r.get("name") or "").strip().lower() == name.strip().lower():
            href = (r.get("meta") or {}).get("href") or ""
            return href.rsplit("/", 1)[-1] if href else None
    # fallback: если не нашли точное — берём первый совпавший
    if rows:
        href = (rows[0].get("meta") or {}).get("href") or ""
        return href.rsplit("/", 1)[-1] if href else None
    return None


def ms_find_assortment_by_article(ms: MoySkladClient, article: str) -> Optional[Dict[str, Any]]:
    rep = ms.request("GET", f"/entity/assortment?search={article}&limit=100")
    rows = rep.get("rows") or []
    for r in rows:
        if str(r.get("article") or "") == str(article):
            return r
    return rows[0] if rows else None


def ms_get_customerorder_by_external(ms: MoySkladClient, external_code: str) -> Optional[Dict[str, Any]]:
    rep = ms.request("GET", f"/entity/customerorder?filter=externalCode={external_code}")
    rows = rep.get("rows") or []
    return rows[0] if rows else None


def build_position(ass: Dict[str, Any], qty: float) -> Dict[str, Any]:
    meta = (ass.get("meta") or {})
    return {
        "quantity": qty,
        "assortment": {
            "meta": {
                "href": meta.get("href"),
                "type": meta.get("type"),
                "mediaType": meta.get("mediaType", "application/json"),
            }
        }
    }


def meta_href(entity: str, entity_id: str) -> Dict[str, Any]:
    return {
        "meta": {
            "href": f"{MS_BASE}/entity/{entity}/{entity_id}",
            "type": entity,
            "mediaType": "application/json",
        }
    }


def main() -> None:
    cfg = load_config()
    ms = MoySkladClient(cfg.moysklad_token)

    supply_order_id = int(os.environ.get("FBO_SUPPLY_ORDER_ID", "0"))
    if not supply_order_id:
        raise SystemExit("Set FBO_SUPPLY_ORDER_ID=<order_id>")

    cabinet = int(os.environ.get("FBO_CABINET", "1"))  # 1 или 2
    dry_run = os.environ.get("FBO_DRY_RUN", "1") == "1"

    headers = ozon_headers(cfg, cabinet)

    rep = ozon_supply_get(headers, supply_order_id)
    orders = rep.get("orders") or []
    if not orders:
        raise SystemExit(f"No orders for order_id={supply_order_id}")

    o = orders[0]
    order_number = o.get("order_number") or str(supply_order_id)
    supplies = o.get("supplies") or []
    if not supplies:
        raise SystemExit("No supplies[] in ozon response")

    bundle_id = supplies[0].get("bundle_id")
    if not bundle_id:
        raise SystemExit("No bundle_id in supplies[0]")

    items = ozon_bundle_items(headers, bundle_id)
    if not items:
        raise SystemExit("Bundle returned 0 items")

    external_code = f"OZON_FBO_SUPPLY_{supply_order_id}"
    exists = ms_get_customerorder_by_external(ms, external_code)
    if exists:
        print(f"[SKIP] customerorder exists: externalCode={external_code}")
        print((exists.get("meta") or {}).get("href"))
        return

    # organization / agent
    org_id = getattr(cfg, "moysklad_org_id", "") or ""
    agent_id = getattr(cfg, "moysklad_ozon_agent_id", "") or ""

    if not org_id:
        org_id = ms_get_first_organization(ms) or ""
    if not agent_id:
        agent_id = ms_find_counterparty_id(ms, "Ozon") or ms_find_counterparty_id(ms, "Озон") or ""

    if not org_id:
        raise SystemExit("MOYSKLAD_ORG_ID is missing and auto-detect failed")
    if not agent_id:
        raise SystemExit("MOYSKLAD_OZON_AGENT_ID is missing and auto-detect failed (create/find counterparty 'Ozon')")

    positions: List[Dict[str, Any]] = []
    missing: List[str] = []

    for it in items:
        offer_id = str(it.get("offer_id") or "").strip()
        qty = float(it.get("quantity") or 0)
        if not offer_id or qty <= 0:
            continue

        ass = ms_find_assortment_by_article(ms, offer_id)
        if not ass:
            missing.append(offer_id)
            continue
        positions.append(build_position(ass, qty))

    if not positions:
        raise SystemExit(f"No positions matched in MoySklad. Missing articles: {missing}")

    payload = {
        "externalCode": external_code,
        "name": f"Ozon FBO supply {order_number}",
        "organization": meta_href("organization", org_id),
        "agent": meta_href("counterparty", agent_id),
        "store": meta_href("store", cfg.moysklad_store_id),
        "positions": positions,
        "description": f"Ozon FBO supply order_id={supply_order_id}, bundle_id={bundle_id}, cabinet={cabinet}",
    }

    if dry_run:
        print("[DRY_RUN] Would create customerorder:")
        print(payload)
        if missing:
            print("[WARN] Missing in MS by article:", ", ".join(missing))
        return

    created = ms.request("POST", "/entity/customerorder", json=payload)
    print("[OK] Created customerorder:", (created.get("meta") or {}).get("href"))


if __name__ == "__main__":
    main()
