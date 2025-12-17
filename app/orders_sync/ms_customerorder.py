from __future__ import annotations

from datetime import datetime
from typing import Any

from .constants import MS_COUNTERPARTY_OZON_ID, MS_STORE_OZON_ID, OZON_TO_MS_STATE
from .ms_meta import ms_meta, ms_state_meta, ms_sales_channel_meta
from .assortment import AssortmentResolver, extract_sale_price_cents

def parse_dt(s: str) -> str:
    # МС принимает ISO; оставляем как есть (Ozon отдаёт Z)
    return s

class CustomerOrderService:
    def __init__(self, ms):
        self.ms = ms
        self.ass = AssortmentResolver(ms)

    def find_by_name(self, name: str) -> dict | None:
        resp = self.ms.get("/entity/customerorder", params={"filter": f'name="{name}"', "limit": 1})
        rows = resp.get("rows") or []
        return rows[0] if rows else None

    def build_positions(self, products: list[dict]) -> list[dict]:
        positions: list[dict] = []
        for p in products:
            offer_id = str(p["offer_id"]).strip()
            qty = float(p.get("quantity") or 0)
            ass = self.ass.get_by_article(offer_id)
            price = extract_sale_price_cents(ass)
            positions.append({
                "assortment": (ass.get("meta") or {}),
                "quantity": qty,
                "price": price,
                "reserve": qty,  # включаем резерв
            })
        return positions

    def upsert_from_ozon(self, *,
                         order_number: str,
                         ozon_status: str,
                         shipment_date: str,
                         products: list[dict],
                         sales_channel_id: str,
                         posting_number: str | None = None) -> dict:
        state_id = OZON_TO_MS_STATE.get(ozon_status)
        if not state_id:
            raise ValueError(f"Unknown ozon status: {ozon_status}")

        payload: dict[str, Any] = {
            "name": order_number,
            "agent": ms_meta("counterparty", MS_COUNTERPARTY_OZON_ID),
            "store": ms_meta("store", MS_STORE_OZON_ID),
            "state": ms_state_meta(state_id),
            "moment": parse_dt(shipment_date),               # Дата заказа = ожидаемая отгрузка
            "shipmentPlannedMoment": parse_dt(shipment_date),
            "salesChannel": ms_sales_channel_meta(sales_channel_id),
            "positions": {"rows": self.build_positions(products)},
            # "description": ""  # комментарий пока пустой
        }

        # полезно сохранять posting_number, но не ломаем “красивый” state
        if posting_number:
            payload["externalCode"] = posting_number

        existing = self.find_by_name(order_number)
        if not existing:
            return self.ms.post("/entity/customerorder", json=payload)

        # PATCH только если отличается — на первом шаге можно просто PUT/POST-merge.
        # Я бы начал с "update whole document" минимально рискованно:
        return self.ms.put(f"/entity/customerorder/{existing['id']}", json=payload)

    def remove_reserve(self, order: dict) -> dict:
        # Снимаем резерв: reserve=0 по всем позициям
        order_id = order["id"]
        # забираем позиции
        pos = self.ms.get(f"/entity/customerorder/{order_id}/positions", params={"limit": 1000})
        rows = pos.get("rows") or []
        patch_rows = [{"id": r["id"], "reserve": 0} for r in rows]
        if patch_rows:
            self.ms.put(f"/entity/customerorder/{order_id}/positions", json={"rows": patch_rows})
        return self.ms.get(f"/entity/customerorder/{order_id}")
