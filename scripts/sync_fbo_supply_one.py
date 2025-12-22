import os
from typing import Any, Dict, List, Optional

from app.config import load_config
from app.http import request_json
from app.moysklad_client import MoySkladClient

OZON_API = "https://api-seller.ozon.ru"


def ozon_headers(cab: Dict[str, Any]) -> Dict[str, str]:
    return {
        "Client-Id": str(cab["ozon_client_id"]),
        "Api-Key": cab["ozon_api_key"],
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


def ms_find_assortment_by_article(ms: MoySkladClient, article: str) -> Optional[Dict[str, Any]]:
    rep = ms.request("GET", f"/entity/assortment?search={article}")
    rows = rep.get("rows") or []
    for r in rows:
        if str(r.get("article") or "") == str(article):
            return r
    return rows[0] if rows else None


def ms_get_customerorder_by_external(ms: MoySkladClient, external_code: str) -> Optional[Dict[str, Any]]:
    rep = ms.request("GET", f"/entity/customerorder?filter=externalCode={external_code}")
    rows = rep.get("rows") or []
    return rows[0] if rows else None


def build_position_from_assortment(ass: Dict[str, Any], qty: float) -> Dict[str, Any]:
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


def main() -> None:
    cfg = load_config()
    ms = MoySkladClient(cfg.moysklad_token)

    # ENV
    supply_order_id = int(os.environ.get("FBO_SUPPLY_ORDER_ID", "0"))
    if not supply_order_id:
        raise SystemExit("Set FBO_SUPPLY_ORDER_ID=<order_id>")

    cabinet_idx = int(os.environ.get("FBO_CABINET_INDEX", "0"))
    dry_run = os.environ.get("FBO_DRY_RUN", "1") == "1"

    cab = cfg.ozon_cabinets[cabinet_idx]  # предполагаю, что у тебя так хранится
    headers = ozon_headers(cab)

    supply_rep = ozon_supply_get(headers, supply_order_id)
    orders = supply_rep.get("orders") or []
    if not orders:
        raise SystemExit(f"No orders found for order_id={supply_order_id}")

    o = orders[0]
    order_number = o.get("order_number")
    supplies = o.get("supplies") or []
    if not supplies:
        raise SystemExit("No supplies[] in response")

    bundle_id = supplies[0].get("bundle_id")
    if not bundle_id:
        raise SystemExit("No bundle_id in supplies[0]")

    items = ozon_bundle_items(headers, bundle_id)
    if not items:
        raise SystemExit("Bundle has no items")

    external_code = f"OZON_FBO_SUPPLY_{supply_order_id}"

    existing = ms_get_customerorder_by_external(ms, external_code)
    if existing:
        print(f"[SKIP] customerorder already exists externalCode={external_code}")
        print(existing.get("meta", {}).get("href"))
        return

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

        positions.append(build_position_from_assortment(ass, qty))

    if missing:
        print("[WARN] Not found in MoySklad by article:", ", ".join(missing))
        # MVP-решение: не падаем, но и не создаём пустой заказ
        if not positions:
            raise SystemExit("All positions missing in MoySklad, aborting")

    payload = {
        "externalCode": external_code,
        "name": f"Ozon FBO supply {order_number}",
        "organization": {"meta": {"href": f"{cfg.moysklad_base_url}/entity/organization/{cfg.moysklad_org_id}", "type": "organization", "mediaType": "application/json"}},
        "agent": {"meta": {"href": f"{cfg.moysklad_base_url}/entity/counterparty/{cfg.moysklad_ozon_agent_id}", "type": "counterparty", "mediaType": "application/json"}},
        "store": {"meta": {"href": f"{cfg.moysklad_base_url}/entity/store/{cfg.moysklad_store_id}", "type": "store", "mediaType": "application/json"}},
        "positions": positions,
        "description": f"Ozon FBO supply order_id={supply_order_id}, bundle_id={bundle_id}",
    }

    if dry_run:
        print("[DRY_RUN] Would create customerorder:")
        print(payload)
        return

    created = ms.request("POST", "/entity/customerorder", json=payload)
    print("[OK] Created customerorder:", created.get("meta", {}).get("href"))


if __name__ == "__main__":
    main()
