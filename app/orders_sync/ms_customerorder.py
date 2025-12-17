from __future__ import annotations

from typing import Any
from datetime import datetime, timezone

from .constants import (
    MS_COUNTERPARTY_OZON_ID,
    MS_STORE_OZON_ID,
    MS_ORGANIZATION_ID,
    OZON_TO_MS_STATE,
)
from .ms_meta import ms_meta, ms_state_meta, ms_sales_channel_meta
from .assortment import AssortmentResolver, extract_sale_price_cents


def parse_dt(s: str) -> str:
    # Ozon: 2025-12-16T13:00:00Z
    # MS:   2025-12-16 13:00:00.000
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


class CustomerOrderService:
    def __init__(self, ms):
        self.ms = ms
        self.ass = AssortmentResolver(ms)

    def find_by_name(self, name: str) -> dict | None:
        name = (name or "").strip()
        if not name:
            return None

        # search (надёжнее, чем filter по name)
        resp = self.ms.get("/entity/customerorder", params={"search": name, "limit": 100})
        rows = resp.get("rows") or []
        for x in rows:
            if (x.get("name") or "").strip() == name:
                return x

        # fallback filter
        for flt in (f'name="{name}"', f"name={name}"):
            resp = self.ms.get("/entity/customerorder", params={"filter": flt, "limit": 50})
            rows = resp.get("rows") or []
            for x in rows:
                if (x.get("name") or "").strip() == name:
                    return x

        return None

    def ensure_prices(self, order: dict) -> None:
        """
        Если в заказе покупателя у строк price=0 — ставим цену продажи из МС.
        Обновляем каждую позицию по meta.href (надёжно).
        """
        order_id = order["id"]
        pos = self.ms.get(f"/entity/customerorder/{order_id}/positions", params={"limit": 1000})
        rows = pos.get("rows") or []

        for r in rows:
            if int(r.get("price") or 0) != 0:
                continue

            ass_meta = ((r.get("assortment") or {}).get("meta") or {})
            href = ass_meta.get("href")
            if not href:
                continue

            ass = self.ms.get(href)
            price = extract_sale_price_cents(ass)
            if price <= 0:
                continue

            # обновляем позицию по meta.href
            rmeta = (r.get("meta") or {})
            rhref = rmeta.get("href")
            if not rhref:
                continue

            self.ms.put(rhref, json={"price": int(price)})

    def build_positions(self, products: list[dict]) -> list[dict]:
        positions: list[dict] = []

        for p in products:
            offer_id = str(p["offer_id"]).strip()
            qty = float(p.get("quantity") or 0)

            ass = self.ass.get_by_article(offer_id)
            price = extract_sale_price_cents(ass)

            positions.append(
                {
                    "assortment": {"meta": ass["meta"]},
                    "quantity": qty,
                    "price": price,
                    "reserve": qty,
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
        if not posting_number:
            raise ValueError("posting_number is required")

        ms_name = posting_number.strip()

        ozon_status = (ozon_status or "").strip().lower()
        state_id = OZON_TO_MS_STATE.get(ozon_status)
        if not state_id:
            raise ValueError(f"Unknown ozon status: {ozon_status}")

        existing = self.find_by_name(ms_name)

        # Создание нового заказа (только один раз)
        if not existing:
            payload: dict[str, Any] = {
                "name": ms_name,
                "organization": ms_meta("organization", MS_ORGANIZATION_ID),
                "agent": ms_meta("counterparty", MS_COUNTERPARTY_OZON_ID),
                "store": ms_meta("store", MS_STORE_OZON_ID),
                "state": ms_state_meta(state_id),
                "moment": parse_dt(shipment_date),
                "shipmentPlannedMoment": parse_dt(shipment_date),
                "salesChannel": ms_sales_channel_meta(sales_channel_id),
                "positions": {"rows": self.build_positions(products)},
                "externalCode": (order_number or "").strip(),  # справочно
            }
            return self.ms.post("/entity/customerorder", json=payload)

        # Обновление существующего заказа: ТОЛЬКО статус
        patch = {"state": ms_state_meta(state_id)}
        return self.ms.put(f"/entity/customerorder/{existing['id']}", json=patch)

    def remove_reserve(self, order: dict) -> dict:
        """
        Снимаем резерв по всем позициям заказа.
        Вызывается ТОЛЬКО при статусе Ozon = cancelled.
        Реализация: PUT каждой позиции по meta.href (единственный стабильный способ).
        """
        order_id = order["id"]

        pos = self.ms.get(
            f"/entity/customerorder/{order_id}/positions",
            params={"limit": 1000},
        )
        rows = pos.get("rows") or []

        for r in rows:
            meta = (r.get("meta") or {})
            href = meta.get("href")
            if not href:
                continue

            # Обновляем конкретную позицию
            self.ms.put(
                href,
                json={"reserve": 0},
            )

        return self.ms.get(f"/entity/customerorder/{order_id}")
