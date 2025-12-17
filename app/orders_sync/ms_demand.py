from __future__ import annotations

from typing import Any, Dict, Optional

from .constants import (
    MS_COUNTERPARTY_OZON_ID,
    MS_STORE_OZON_ID,
    MS_DEMAND_STATE_FBS_ID,
    MS_ORGANIZATION_ID,
)
from .ms_meta import ms_meta, ms_demand_state_meta, ms_sales_channel_meta


class DemandService:
    def __init__(self, ms):
        self.ms = ms

    def find_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        resp = self.ms.get(
            "/entity/demand",
            params={"filter": f'externalCode="{external_code}"', "limit": 1},
        )
        rows = resp.get("rows") or []
        return rows[0] if rows else None

    def list_by_external_code(self, external_code: str) -> list[dict]:
        resp = self.ms.get(
            "/entity/demand",
            params={"filter": f'externalCode="{external_code}"', "limit": 100},
        )
        return resp.get("rows") or []

    def delete_demand(self, demand_id: str) -> None:
        self.ms.delete(f"/entity/demand/{demand_id}")

    def ensure_single_demand(self, posting_number: str) -> dict | None:
        demands = self.list_by_external_code(posting_number)
        if not demands:
            return None

        # оставим самую раннюю по moment (или первую)
        def key(d: dict):
            return d.get("moment") or ""

        demands_sorted = sorted(demands, key=key)
        keep = demands_sorted[0]
        extras = demands_sorted[1:]

        for d in extras:
            did = d.get("id")
            if did:
                self.delete_demand(did)

        return keep

    def ensure_prices(self, demand: dict) -> None:
        """
        Если в отгрузке price=0 — ставим цену продажи из МС.
        Обновляем каждую позицию по meta.href.
        """
        demand_id = demand["id"]
        pos = self.ms.get(f"/entity/demand/{demand_id}/positions", params={"limit": 1000})
        rows = pos.get("rows") or []

        for r in rows:
            if int(r.get("price") or 0) != 0:
                continue

            ass_meta = ((r.get("assortment") or {}).get("meta") or {})
            href = ass_meta.get("href")
            if not href:
                continue

            ass = self.ms.get(href)
            from .assortment import extract_sale_price_cents
            price = extract_sale_price_cents(ass)
            if price <= 0:
                continue

            rmeta = (r.get("meta") or {})
            rhref = rmeta.get("href")
            if not rhref:
                continue

            self.ms.put(rhref, json={"price": int(price)})

    def create_from_customerorder_if_missing(
        self,
        *,
        customerorder: Dict[str, Any],
        posting_number: str,
        sales_channel_id: str,
        demand_state_id: str = MS_DEMAND_STATE_FBS_ID,
    ) -> Dict[str, Any]:
        """
        Создаём Demand (Отгрузку) из CustomerOrder.
        Идемпотентно:
        - externalCode = posting_number
        - если отгрузок несколько → оставляем одну, остальные удаляем
        - в оставшейся / созданной проставляем цену, если price = 0
        """

        # 1. Если уже есть отгрузки — оставляем одну
        existing = self.ensure_single_demand(posting_number)
        if existing:
            self.ensure_prices(existing)
            return existing

        # 2. Создаём новую отгрузку
        co_meta = customerorder.get("meta")
        if not co_meta:
            raise ValueError("customerorder.meta is missing (cannot create demand)")

        payload: Dict[str, Any] = {
            "externalCode": posting_number,
            "agent": ms_meta("counterparty", MS_COUNTERPARTY_OZON_ID),
            "store": ms_meta("store", MS_STORE_OZON_ID),
            "organization": ms_meta("organization", MS_ORGANIZATION_ID),
            "customerOrder": {"meta": co_meta},
            "state": ms_demand_state_meta(demand_state_id),
            "salesChannel": ms_sales_channel_meta(sales_channel_id),
        }

        created = self.ms.post("/entity/demand", json=payload)

        # 3. Лечим цены в новой отгрузке
        self.ensure_prices(created)

        return created
