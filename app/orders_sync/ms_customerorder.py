from __future__ import annotations

from typing import Any

from .constants import (
    MS_COUNTERPARTY_OZON_ID,
    MS_STORE_OZON_ID,
    OZON_TO_MS_STATE,
    MS_ORGANIZATION_ID,
)
from .ms_meta import ms_meta, ms_state_meta, ms_sales_channel_meta
from .assortment import AssortmentResolver, extract_sale_price_cents

from datetime import datetime, timezone

def parse_dt(s: str) -> str:
    # Ozon: 2025-12-16T13:00:00Z
    # MS wants: 2025-12-16 13:00:00.000
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

class CustomerOrderService:
    def __init__(self, ms):
        self.ms = ms
        self.ass = AssortmentResolver(ms)

    def find_by_name(self, name: str) -> dict | None:
        resp = self.ms.get(
            "/entity/customerorder",
            params={"filter": f'name="{name}"', "limit": 1},
        )
        rows = resp.get("rows") or []
        return rows[0] if rows else None

    def find_by_external_code(self, external_code: str) -> dict | None:
        resp = self.ms.get(
            "/entity/customerorder",
            params={"filter": f'externalCode="{external_code}"', "limit": 1},
        )
        rows = resp.get("rows") or []
        return rows[0] if rows else None

    # === используется ТОЛЬКО при создании заказа ===
    def build_positions(self, products: list[dict]) -> list[dict]:
        positions: list[dict] = []
        for p in products:
            offer_id = str(p["offer_id"]).strip()
            qty = float(p.get("quantity") or 0)

            ass = self.ass.get_by_article(offer_id)
            price = extract_sale_price_cents(ass)

            positions.append(
                {
                    "assortment": {"meta": (ass.get("meta") or {})},
                    "quantity": qty,
                    "price": price,
                    "reserve": qty,  # включаем резерв
                }
            )
        return positions

    def upsert_from_ozon(
        self,
        order_number: str,
        ozon_status: str,
        shipment_date: str,
        products: list[dict],
        sales_channel_id: str,
        posting_number: str | None = None,
    ) -> dict:
        state_id = OZON_TO_MS_STATE.get(ozon_status)
        if not state_id:
            raise ValueError(f"Unknown ozon status: {ozon_status}")

        existing = None
        if posting_number:
            existing = self.find_by_external_code(posting_number)
        if not existing:
            existing = self.find_by_name(order_number)

        # ======================================================
        # 1) ЗАКАЗА НЕТ → СОЗДАЁМ ПОЛНОСТЬЮ
        # ======================================================
        if not existing:
            payload: dict[str, Any] = {
                "name": order_number,
                "agent": ms_meta("counterparty", MS_COUNTERPARTY_OZON_ID),
                "organization": ms_meta("organization", MS_ORGANIZATION_ID),
                "store": ms_meta("store", MS_STORE_OZON_ID),
                "state": ms_state_meta(state_id),
                "moment": parse_dt(shipment_date),               # Дата заказа
                "shipmentPlannedMoment": parse_dt(shipment_date),
                "salesChannel": ms_sales_channel_meta(sales_channel_id),
                "positions": {"rows": self.build_positions(products)},
                # "description": ""  # комментарий пока пустой
            }

            if posting_number:
                payload["externalCode"] = posting_number

            return self.ms.post("/entity/customerorder", json=payload)

        # ======================================================
        # 2) ЗАКАЗ УЖЕ ЕСТЬ → ОБНОВЛЯЕМ ТОЛЬКО СТАТУС
        # ======================================================
        patch = {
            "state": ms_state_meta(state_id),
        }

        return self.ms.put(
            f"/entity/customerorder/{existing['id']}",
            json=patch,
        )

    def remove_reserve(self, order: dict) -> dict:
        """
        Снимаем резерв по всем позициям заказа
        """
        order_id = order["id"]

        pos = self.ms.get(
            f"/entity/customerorder/{order_id}/positions",
            params={"limit": 1000},
        )
        rows = pos.get("rows") or []

        patch_rows = [{"id": r["id"], "reserve": 0} for r in rows]

        if patch_rows:
            self.ms.put(
                f"/entity/customerorder/{order_id}/positions",
                json={"rows": patch_rows},
            )

        return self.ms.get(f"/entity/customerorder/{order_id}")
