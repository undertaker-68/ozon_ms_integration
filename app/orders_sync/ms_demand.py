from __future__ import annotations

from typing import Any, Dict, Optional

from .constants import (
    MS_COUNTERPARTY_OZON_ID,
    MS_STORE_OZON_ID,
    MS_DEMAND_STATE_FBS_ID,
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
        Идемпотентно: externalCode = posting_number, по нему проверяем дубликаты.
        """
        existing = self.find_by_external_code(posting_number)
        if existing:
            return existing

        payload: Dict[str, Any] = {
            "externalCode": posting_number,
            "agent": ms_meta("counterparty", MS_COUNTERPARTY_OZON_ID),
            "store": ms_meta("store", MS_STORE_OZON_ID),
            "customerOrder": (customerorder.get("meta") or {}),
            "state": ms_demand_state_meta(demand_state_id),
            "salesChannel": ms_sales_channel_meta(sales_channel_id),
            # комментарий пока пустой по твоему требованию
            # "description": ""
        }

        return self.ms.post("/entity/demand", json=payload)
