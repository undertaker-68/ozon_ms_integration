import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
from dateutil import parser as dtparser
from dotenv import load_dotenv

OZON_BASE_URL = os.getenv("OZON_BASE_URL", "https://api-seller.ozon.ru").strip()
MS_BASE_URL = "https://api.moysklad.ru/api/remap/1.2"

# отгрузки/поставки от 22.12.2025 включительно
CUTOFF_DATE = dtparser.isoparse("2025-12-22T00:00:00+00:00").date()


@dataclass
class Cabinet:
    name: str
    ozon_client_id: str
    ozon_api_key: str
    ms_saleschannel_id: str


def die(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def getenv_first(*keys: str, required: bool = True) -> str:
    """
    Берёт первое непустое значение из списка ключей окружения.
    """
    for k in keys:
        v = os.getenv(k)
        if v and v.strip():
            return v.strip()
    if required:
        die(f"Не найдено ни одно из env: {', '.join(keys)}")
    return ""


def getenv_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip() in ("1", "true", "True", "yes", "YES", "on", "ON")


def ozon_headers(cab: Cabinet) -> Dict[str, str]:
    return {
        "Client-Id": cab.ozon_client_id,
        "Api-Key": cab.ozon_api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def ms_headers(ms_token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {ms_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def http_post_json(url: str, headers: Dict[str, str], payload: Dict[str, Any], timeout: int = 60) -> Dict[str, Any]:
    r = requests.post(url, headers=headers, json=payload, timeout=timeout)
    if r.status_code >= 400:
        raise RuntimeError(f"POST {url} -> {r.status_code} {r.text}")
    return r.json() if r.text else {}


def http_get_json(url: str, headers: Dict[str, str], params: Optional[Dict[str, Any]] = None, timeout: int = 60) -> Dict[str, Any]:
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    if r.status_code >= 400:
        raise RuntimeError(f"GET {url} -> {r.status_code} {r.text}")
    return r.json() if r.text else {}


def http_put_json(url: str, headers: Dict[str, str], payload: Dict[str, Any], timeout: int = 60) -> Dict[str, Any]:
    r = requests.put(url, headers=headers, json=payload, timeout=timeout)
    if r.status_code >= 400:
        raise RuntimeError(f"PUT {url} -> {r.status_code} {r.text}")
    return r.json() if r.text else {}


def http_delete(url: str, headers: Dict[str, str], timeout: int = 60) -> None:
    r = requests.delete(url, headers=headers, timeout=timeout)
    if r.status_code >= 400:
        raise RuntimeError(f"DELETE {url} -> {r.status_code} {r.text}")


# ---------------------- OZON ----------------------

def ozon_supply_order_list_v3(cab: Cabinet) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    last_id: Optional[str] = None

    while True:
        payload: Dict[str, Any] = {"limit": 1000}
        if last_id:
            payload["last_id"] = last_id

        data = http_post_json(f"{OZON_BASE_URL}/v3/supply-order/list", ozon_headers(cab), payload)

        items = (
            data.get("result", {}).get("items")
            or data.get("result", {}).get("supply_orders")
            or data.get("items")
            or []
        )
        out.extend(items)

        has_next = data.get("result", {}).get("has_next", data.get("has_next", False))
        new_last_id = data.get("result", {}).get("last_id", data.get("last_id"))

        if not has_next:
            break
        last_id = str(new_last_id) if new_last_id is not None else None
        if not last_id:
            break

        time.sleep(0.2)

    return out


def ozon_supply_order_get_v3(cab: Cabinet, order_id: int) -> Dict[str, Any]:
    return http_post_json(f"{OZON_BASE_URL}/v3/supply-order/get", ozon_headers(cab), {"order_id": order_id})


def ozon_supply_order_bundle_v1(cab: Cabinet, bundle_id: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    last_id: Optional[str] = None

    while True:
        payload: Dict[str, Any] = {"bundle_id": bundle_id, "limit": 1000}
        if last_id:
            payload["last_id"] = last_id

        data = http_post_json(f"{OZON_BASE_URL}/v1/supply-order/bundle", ozon_headers(cab), payload)

        chunk = data.get("items") or data.get("result", {}).get("items") or []
        items.extend(chunk)

        has_next = data.get("has_next", data.get("result", {}).get("has_next", False))
        new_last_id = data.get("last_id", data.get("result", {}).get("last_id"))

        if not has_next:
            break
        last_id = str(new_last_id) if new_last_id is not None else None
        if not last_id:
            break

        time.sleep(0.2)

    return items


def ozon_supply_order_timeslot_get_v1(cab: Cabinet, order_id: int) -> Optional[Any]:
    try:
        data = http_post_json(
            f"{OZON_BASE_URL}/v1/supply-order/timeslot/get",
            ozon_headers(cab),
            {"order_id": order_id},
        )
    except Exception:
        return None

    candidates: List[str] = []
    if isinstance(data, dict):
        r = data.get("result", data)

        for key in ["timeslots", "time_slots", "slots", "items"]:
            v = r.get(key) if isinstance(r, dict) else None
            if isinstance(v, list):
                for s in v:
                    if isinstance(s, dict):
                        for k in ["from", "start", "start_time", "begin", "date"]:
                            if s.get(k):
                                candidates.append(str(s[k]))

        if isinstance(r, dict):
            for k in ["from", "start", "start_time", "begin", "date"]:
                if r.get(k):
                    candidates.append(str(r[k]))

    for v in candidates:
        try:
            return dtparser.isoparse(v).date()
        except Exception:
            continue
    return None


def extract_destination_name(order_get: Dict[str, Any]) -> str:
    wh = order_get.get("result", {}).get("drop_off_warehouse") or order_get.get("drop_off_warehouse") or {}
    if isinstance(wh, dict):
        return str(wh.get("name") or wh.get("warehouse_id") or "").strip()
    return ""


def extract_supply_no(order_get: Dict[str, Any]) -> str:
    r = order_get.get("result", {}) if isinstance(order_get.get("result", {}), dict) else order_get
    if r.get("order_number"):
        return str(r["order_number"])
    supplies = r.get("supplies") or []
    if isinstance(supplies, list) and supplies:
        s0 = supplies[0]
        if isinstance(s0, dict) and s0.get("supply_id"):
            return str(s0["supply_id"])
    if r.get("order_id"):
        return str(r["order_id"])
    return ""


def extract_bundle_ids(order_get: Dict[str, Any]) -> List[str]:
    r = order_get.get("result", {}) if isinstance(order_get.get("result", {}), dict) else order_get
    supplies = r.get("supplies") or []
    out: List[str] = []
    if isinstance(supplies, list):
        for s in supplies:
            if isinstance(s, dict) and s.get("bundle_id"):
                out.append(str(s["bundle_id"]))
    return out


def extract_timeslot_date(order_get: Dict[str, Any]) -> Optional[Any]:
    ts = order_get.get("result", {}).get("timeslot") or order_get.get("timeslot") or {}
    if not isinstance(ts, dict):
        return None
    for k in ["from", "from_time", "start", "start_time", "date", "timeslot", "begin"]:
        v = ts.get(k)
        if v:
            try:
                return dtparser.isoparse(v).date()
            except Exception:
                pass
    return None


def extract_created_date(order_get: Dict[str, Any]) -> Optional[Any]:
    r = order_get.get("result", {}) if isinstance(order_get.get("result", {}), dict) else order_get
    cd = r.get("created_date")
    if not cd:
        return None
    try:
        return dtparser.isoparse(cd).date()
    except Exception:
        return None


# ---------------------- MOYSKLAD ----------------------

def ms_find_assortment_by_article(ms_token: str, article: str) -> Optional[Dict[str, Any]]:
    for entity in ["product", "bundle"]:
        url = f"{MS_BASE_URL}/entity/{entity}"
        params = {"filter": f"article={article}"}
        data = http_get_json(url, ms_headers(ms_token), params=params)
        rows = data.get("rows") or []
        if rows:
            return rows[0]
    return None


def ms_get_bundle_components(ms_token: str, bundle_id: str) -> List[Dict[str, Any]]:
    url = f"{MS_BASE_URL}/entity/bundle/{bundle_id}"
    params = {"expand": "components.assortment"}
    data = http_get_json(url, ms_headers(ms_token), params=params)
    comps = data.get("components") or []
    return comps if isinstance(comps, list) else []


def ms_get_sale_price_cents(assortment: Dict[str, Any]) -> int:
    sale_prices = assortment.get("salePrices") or []
    if isinstance(sale_prices, list):
        for p in sale_prices:
            try:
                if (p.get("priceType") or {}).get("name") == "Цена продажи":
                    return int(p.get("value"))
            except Exception:
                pass
        try:
            return int(sale_prices[0].get("value"))
        except Exception:
            pass
    return 0


def ms_find_customerorder_by_external_code(ms_token: str, external_code: str) -> Optional[Dict[str, Any]]:
    url = f"{MS_BASE_URL}/entity/customerorder"
    params = {"filter": f"externalCode={external_code}"}
    data = http_get_json(url, ms_headers(ms_token), params=params)
    rows = data.get("rows") or []
    return rows[0] if rows else None


def ms_get_order_positions(ms_token: str, order_id: str) -> List[Dict[str, Any]]:
    url = f"{MS_BASE_URL}/entity/customerorder/{order_id}/positions"
    data = http_get_json(url, ms_headers(ms_token))
    return data.get("rows") or []


def ms_delete_position(ms_token: str, order_id: str, pos_id: str) -> None:
    url = f"{MS_BASE_URL}/entity/customerorder/{order_id}/positions/{pos_id}"
    http_delete(url, ms_headers(ms_token))


def ms_add_positions(ms_token: str, order_id: str, positions: List[Dict[str, Any]]) -> None:
    url = f"{MS_BASE_URL}/entity/customerorder/{order_id}/positions"
    http_post_json(url, ms_headers(ms_token), positions)


def ms_create_customerorder(
    ms_token: str,
    name: str,
    external_code: str,
    agent_id: str,
    org_id: str,
    store_id: str,
    state_id: str,
    saleschannel_id: str,
    description: str,
) -> Dict[str, Any]:
    url = f"{MS_BASE_URL}/entity/customerorder"
    payload = {
        "name": name,
        "externalCode": external_code,
        "organization": {"meta": {"href": f"{MS_BASE_URL}/entity/organization/{org_id}", "type": "organization", "mediaType": "application/json"}},
        "agent": {"meta": {"href": f"{MS_BASE_URL}/entity/counterparty/{agent_id}", "type": "counterparty", "mediaType": "application/json"}},
        "store": {"meta": {"href": f"{MS_BASE_URL}/entity/store/{store_id}", "type": "store", "mediaType": "application/json"}},
        "state": {"meta": {"href": f"{MS_BASE_URL}/entity/customerorder/metadata/states/{state_id}", "type": "state", "mediaType": "application/json"}},
        "salesChannel": {"meta": {"href": f"{MS_BASE_URL}/entity/saleschannel/{saleschannel_id}", "type": "saleschannel", "mediaType": "application/json"}},
        "description": description,
    }
    return http_post_json(url, ms_headers(ms_token), payload)


def ms_update_customerorder_meta(ms_token: str, order_id: str, state_id: str, saleschannel_id: str, description: str) -> None:
    url = f"{MS_BASE_URL}/entity/customerorder/{order_id}"
    payload = {
        "state": {"meta": {"href": f"{MS_BASE_URL}/entity/customerorder/metadata/states/{state_id}", "type": "state", "mediaType": "application/json"}},
        "salesChannel": {"meta": {"href": f"{MS_BASE_URL}/entity/saleschannel/{saleschannel_id}", "type": "saleschannel", "mediaType": "application/json"}},
        "description": description,
    }
    http_put_json(url, ms_headers(ms_token), payload)


def build_ms_positions_from_ozon_items(ms_token: str, ozon_items: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int, int]:
    positions: List[Dict[str, Any]] = []
    skipped = 0
    expanded = 0

    for it in ozon_items:
        offer_id = str(it.get("offer_id") or "").strip()  # offer_id -> article
        qty = it.get("quantity")
        if not offer_id or not qty:
            skipped += 1
            continue

        ms_item = ms_find_assortment_by_article(ms_token, offer_id)
        if not ms_item:
            skipped += 1
            continue

        entity_type = (ms_item.get("meta") or {}).get("type")  # product/bundle

        if entity_type == "bundle":
            expanded += 1
            bundle_id = ms_item.get("id")
            comps = ms_get_bundle_components(ms_token, bundle_id)
            for c in comps:
                comp_qty = c.get("quantity")
                ass = c.get("assortment") or {}
                if not comp_qty or not ass or not (ass.get("meta") or {}):
                    continue
                total_qty = float(qty) * float(comp_qty)
                price = ms_get_sale_price_cents(ass)
                positions.append({"quantity": total_qty, "price": price, "assortment": {"meta": ass["meta"]}})
            continue

        price = ms_get_sale_price_cents(ms_item)
        positions.append({"quantity": qty, "price": price, "assortment": {"meta": (ms_item.get("meta") or {})}})

    return positions, skipped, expanded


def main() -> None:
    load_dotenv()

    dry_run = getenv_bool("FBO_DRY_RUN", default=False)

    # читаем ТВОИ имена env (без ломания текущего .env)
    ms_token = getenv_first("MS_TOKEN", "MOYSKLAD_TOKEN")
    ms_store_id = getenv_first("MS_STORE_ID", "MOYSKLAD_STORE_ID")
    ms_org_id = getenv_first("MS_ORG_ID", "MS_ORGANIZATION_ID", "MOYSKLAD_ORG_ID", "MS_ORGANIZATION_ID")
    ms_agent_id = getenv_first("MS_AGENT_ID", "MS_COUNTERPARTY_OZON_ID", "MOYSKLAD_OZON_COUNTERPARTY_ID")
    ms_state_id = getenv_first("MS_STATE_ID")  # это добавь, если нет
    ms_sc1 = getenv_first("MS_SALESCHANNEL_ID_CAB1")
    ms_sc2 = getenv_first("MS_SALESCHANNEL_ID_CAB2")

    cab1 = Cabinet(
        name="cab1",
        ozon_client_id=getenv_first("OZON1_CLIENT_ID"),
        ozon_api_key=getenv_first("OZON1_API_KEY"),
        ms_saleschannel_id=ms_sc1,
    )
    cab2 = Cabinet(
        name="cab2",
        ozon_client_id=getenv_first("OZON2_CLIENT_ID"),
        ozon_api_key=getenv_first("OZON2_API_KEY"),
        ms_saleschannel_id=ms_sc2,
    )

    created = updated = 0
    skipped_by_date = 0
    skipped_items_total = 0
    expanded_bundles_total = 0
    errors = 0

    print(f"DRY_RUN={int(dry_run)} | CUTOFF_DATE>={CUTOFF_DATE.isoformat()} | OZON_BASE_URL={OZON_BASE_URL}")

    for cab in [cab1, cab2]:
        print(f"\n=== OZON {cab.name}: list supply-orders ===")
        try:
            orders = ozon_supply_order_list_v3(cab)
        except Exception as e:
            print(f"[{cab.name}] list error: {e}")
            errors += 1
            continue

        print(f"[{cab.name}] найдено заявок: {len(orders)}")

        for o in orders:
            order_id = o.get("order_id") or o.get("id")
            if order_id is None:
                continue

            try:
                og = ozon_supply_order_get_v3(cab, int(order_id))
            except Exception as e:
                print(f"[{cab.name}] get order_id={order_id} error: {e}")
                errors += 1
                continue

            supply_no = extract_supply_no(og) or str(o.get("order_number") or o.get("order_id") or order_id)
            dest = extract_destination_name(og) or ""
            description = f"{supply_no} - {dest}".strip(" -")  # комментарий: <номер> - <склад назначения>

            # план. дата = timeslot; если пусто -> timeslot/get; если пусто -> created_date
            ts_date = extract_timeslot_date(og)
            if ts_date is None:
                ts_date = ozon_supply_order_timeslot_get_v1(cab, int(order_id))
            if ts_date is None:
                ts_date = extract_created_date(og)

            if ts_date is None or ts_date < CUTOFF_DATE:
                skipped_by_date += 1
                continue

            bundle_ids = extract_bundle_ids(og)
            ozon_items: List[Dict[str, Any]] = []
            try:
                for bid in bundle_ids:
                    ozon_items.extend(ozon_supply_order_bundle_v1(cab, bid))
            except Exception as e:
                print(f"[{cab.name}] bundle items error supply={supply_no}: {e}")
                errors += 1
                continue

            positions, skipped_items, expanded_bundles = build_ms_positions_from_ozon_items(ms_token, ozon_items)
            skipped_items_total += skipped_items
            expanded_bundles_total += expanded_bundles

            if not positions:
                print(f"[{cab.name}] supply={supply_no}: нет позиций после маппинга (всё пропущено)")
                continue

            if dry_run:
                print(f"[{cab.name}] WOULD_SYNC supply={supply_no} date={ts_date} positions={len(positions)} comment='{description}' skippedItems={skipped_items}")
                continue

            try:
                existing = ms_find_customerorder_by_external_code(ms_token, supply_no)
                if not existing:
                    order = ms_create_customerorder(
                        ms_token=ms_token,
                        name=supply_no,               # номер заказа = номер поставки
                        external_code=supply_no,      # защита от дублей
                        agent_id=ms_agent_id,
                        org_id=ms_org_id,
                        store_id=ms_store_id,
                        state_id=ms_state_id,
                        saleschannel_id=cab.ms_saleschannel_id,
                        description=description,
                    )
                    order_id_ms = order["id"]
                    ms_add_positions(ms_token, order_id_ms, positions)
                    created += 1
                    print(f"[{cab.name}] CREATED supply={supply_no} positions={len(positions)} skippedItems={skipped_items} bundlesExpanded={expanded_bundles}")
                else:
                    order_id_ms = existing["id"]
                    ms_update_customerorder_meta(ms_token, order_id_ms, ms_state_id, cab.ms_saleschannel_id, description)

                    old_positions = ms_get_order_positions(ms_token, order_id_ms)
                    for p in old_positions:
                        pid = p.get("id")
                        if pid:
                            ms_delete_position(ms_token, order_id_ms, pid)

                    ms_add_positions(ms_token, order_id_ms, positions)
                    updated += 1
                    print(f"[{cab.name}] UPDATED supply={supply_no} positions={len(positions)} skippedItems={skipped_items} bundlesExpanded={expanded_bundles}")

            except Exception as e:
                print(f"[{cab.name}] MS upsert error supply={supply_no}: {e}")
                errors += 1

            time.sleep(0.15)

    print("\n=== RESULT ===")
    print(f"created: {created}")
    print(f"updated: {updated}")
    print(f"skipped_by_date: {skipped_by_date}")
    print(f"skipped_items_total (not found in MS): {skipped_items_total}")
    print(f"expanded_bundles_total: {expanded_bundles_total}")
    print(f"errors: {errors}")


if __name__ == "__main__":
    main()
