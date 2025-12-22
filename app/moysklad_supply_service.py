from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from app.http import request_json, HttpError

# Подключение всех сущностей (например, если они в meta)
def ms_meta(entity: str, uuid: str) -> Dict[str, Any]:
    return {
        "meta": {
            "href": f"https://api.moysklad.ru/api/remap/1.2/entity/{entity}/{uuid}",
            "type": entity,
            "mediaType": "application/json",
        }
    }

def ms_moment_from_date(date_yyyy_mm_dd: str, hh: str = "00", mm: str = "00", ss: str = "00") -> str:
    return f"{date_yyyy_mm_dd} {hh}:{mm}:{ss}.000"

@dataclass(frozen=True)
class MsFboConfig:
    organization_id: str
    counterparty_ozon_id: str
    store_src_id: str  # склад-источник (откуда перемещаем)
    store_fbo_id: str  # склад-назначение (FBO)
    state_customerorder_fbo_id: str
    state_move_supply_id: str
    state_demand_fbo_id: str
    sales_channel_id: str  # кабинет-зависимый

class MoySkladSupplyService:
    def __init__(self, *, ms_token: str, cfg: MsFboConfig) -> None:
        self.ms_token = ms_token
        self.cfg = cfg
        self.base = "https://api.moysklad.ru/api/remap/1.2"

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.ms_token}",
            "Content-Type": "application/json",
            "Accept": "application/json;charset=utf-8",
        }

    def find_customerorder_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base}/entity/customerorder"
        params = {"filter": f"externalCode={external_code}"}
        rep = request_json("GET", url, headers=self._headers(), params=params, timeout=90)
        rows = rep.get("rows") or []
        return rows[0] if rows else None

    def update_customerorder(self, customerorder_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base}/entity/customerorder/{customerorder_id}"
        return request_json("PUT", url, headers=self._headers(), json_body=payload, timeout=90)

    def create_customerorder(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base}/entity/customerorder"
        return request_json("POST", url, headers=self._headers(), json_body=payload, timeout=90)

    def _expand_items_to_component_positions(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        agg_qty: Dict[str, float] = {}  
        meta_by_href: Dict[str, Dict[str, Any]] = {}
        price_by_href: Dict[str, int] = {}

        def remember(href: str, meta: Dict[str, Any], qty: float, price: int) -> None:
            if not href or qty <= 0:
                return
            agg_qty[href] = agg_qty.get(href, 0.0) + qty
            if href not in meta_by_href and meta:
                meta_by_href[href] = meta
            if href not in price_by_href:
                price_by_href[href] = int(price or 0)

        for it in items:
            offer_id = str(it.get("offer_id") or "").strip()
            qty = float(it.get("quantity") or 0)
            if not offer_id or qty <= 0:
                continue
            a = self.find_assortment_by_article(offer_id)
            if not a:
                continue
            a_meta = (a.get("meta") or {})
            a_type = str(a_meta.get("type") or "").lower()
            a_href = str(a_meta.get("href") or "").strip()
            a_price = self._pick_sale_price_value(a)

            if a_type == "bundle" and a_href:
                bundle_id = a_href.rstrip("/").split("/")[-1]
                b = self.get_bundle(bundle_id)
                comps = ((b or {}).get("components") or {}).get("rows") or []
                for c in comps:
                    c_qty = float(c.get("quantity") or 0)
                    c_meta = ((c.get("assortment") or {}).get("meta") or {})
                    c_href = str(c_meta.get("href") or "").strip()
                    if not c_href or c_qty <= 0:
                        continue
                    remember(c_href, c_meta, qty * c_qty, 0)  # Price can be set later
            else:
                remember(a_href, a_meta, qty, a_price)

        positions: List[Dict[str, Any]] = []
        for href, q in agg_qty.items():
            meta = meta_by_href.get(href) or {}
            price = int(price_by_href.get(href) or 0)
            if not meta:
                parts = href.rstrip("/").split("/")
                ent_type = parts[-2]
                meta = {"href": href, "type": ent_type, "mediaType": "application/json"}
            positions.append({"assortment": {"meta": meta}, "quantity": q, "price": price})
        return positions

    def upsert_customerorder(self, *, order_number: str, shipment_planned_moment: str, core: Dict[str, Any], items: List[Dict[str, Any]], dry_run: bool) -> Dict[str, Any]:
        existing = self.find_customerorder_by_external_code(order_number)
        payload: Dict[str, Any] = {
            "externalCode": order_number,
            "name": order_number,
            "organization": ms_meta("organization", self.cfg.organization_id),
            "agent": ms_meta("counterparty", self.cfg.counterparty_ozon_id),
            "store": ms_meta("store", self.cfg.store_fbo_id),
            "state": ms_meta("state", self.cfg.state_customerorder_fbo_id),
            "salesChannel": ms_meta("saleschannel", self.cfg.sales_channel_id),
            "applicable": True,
        }
        if shipment_planned_moment:
            payload["shipmentPlannedMoment"] = shipment_planned_moment

        if dry_run:
            return existing or {"id": "DRYRUN", "meta": {"href": "DRYRUN"}}

        if existing:
            co = self.update_customerorder(existing["id"], payload)
        else:
            payload["description"] = self._build_comment_once(order_number, core)
            co = self.create_customerorder(payload)

        positions = self._expand_items_to_component_positions(items)
        self.replace_customerorder_positions(co["id"], positions)

        return co
