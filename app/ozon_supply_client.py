from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from app.http import request_json

OZON_SUPPLY_ORDER_LIST = "/v3/supply-order/list"
OZON_SUPPLY_ORDER_GET = "/v3/supply-order/get"
OZON_SUPPLY_ORDER_BUNDLE = "/v1/supply-order/bundle"


@dataclass(frozen=True)
class OzonCabinet:
    name: str
    base_url: str  # обычно https://api-seller.ozon.ru
    api_key: str
    client_id: str


class OzonSupplyClient:
    def __init__(self, cabinet: OzonCabinet) -> None:
        self.cabinet = cabinet

    def _headers(self) -> Dict[str, str]:
        return {
            "Client-Id": self.cabinet.client_id,
            "Api-Key": self.cabinet.api_key,
            "Content-Type": "application/json",
        }

    def supply_order_list(
        self,
        *,
        states: List[str],
        limit: int = 100,
        sort_by: str = "ORDER_CREATION",
        sort_dir: str = "DESC",
        last_id: str = "",
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "filter": {"states": states},
            "limit": limit,
            "sort_by": sort_by,
            "sort_dir": sort_dir,
            "last_id": last_id,
        }
        return request_json(
            "POST",
            self.cabinet.base_url + OZON_SUPPLY_ORDER_LIST,
            headers=self._headers(),
            json_body=payload,
            timeout=60,
        )

    def supply_order_get(self, *, order_ids: List[int]) -> Dict[str, Any]:
        payload = {"order_ids": [str(x) for x in order_ids]}
        return request_json(
            "POST",
            self.cabinet.base_url + OZON_SUPPLY_ORDER_GET,
            headers=self._headers(),
            json_body=payload,
            timeout=60,
        )

    def supply_order_bundle(self, *, bundle_ids: List[str], limit: int = 100, last_id: str = "") -> Dict[str, Any]:
        payload: Dict[str, Any] = {"bundle_ids": bundle_ids, "limit": limit, "last_id": last_id}
        return request_json(
            "POST",
            self.cabinet.base_url + OZON_SUPPLY_ORDER_BUNDLE,
            headers=self._headers(),
            json_body=payload,
            timeout=60,
        )

    def iter_supply_orders_full(self, *, states: List[str], limit: int = 100, batch_get: int = 50) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        last_id = ""
        while True:
            rep = self.supply_order_list(states=states, limit=limit, last_id=last_id)
            order_ids = rep.get("order_ids") or []
            new_last_id = rep.get("last_id") or ""

            if not order_ids:
                break

            for i in range(0, len(order_ids), batch_get):
                chunk = order_ids[i : i + batch_get]
                got = self.supply_order_get(order_ids=[int(x) for x in chunk])
                out.extend(got.get("orders") or [])

            if not new_last_id or new_last_id == last_id:
                break
            last_id = new_last_id

        return out

    # ----------------- IMPORTANT FOR пункт 3 -----------------

    def iter_bundle_items(self, bundle_id: str) -> List[Dict[str, Any]]:
        """
        Ozon: /v1/supply-order/bundle
        Возвращает позиции внутри bundle_id.

        На выходе даем список raw items, дальше нормализуем в get_supply_order_items.
        """
        bundle_id = (bundle_id or "").strip()
        if not bundle_id:
            return []

        out: List[Dict[str, Any]] = []
        last_id = ""
        while True:
            rep = self.supply_order_bundle(bundle_ids=[bundle_id], limit=100, last_id=last_id)

            # у Озона встречались разные ключи, поэтому страхуемся
            items = rep.get("items") or rep.get("rows") or rep.get("result") or []
            if isinstance(items, dict):
                # иногда result может быть объектом
                items = items.get("items") or []

            if items:
                out.extend(items)

            new_last_id = rep.get("last_id") or ""
            if not new_last_id or new_last_id == last_id:
                break
            last_id = new_last_id

        return out

    def get_supply_order_items(self, order: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Нормализуем позиции поставки к виду:
        [{"offer_id": "...", "quantity": N}, ...]

        Берем bundle_id из order.supplies[0].bundle_id
        """
        supplies = order.get("supplies") or []
        s0 = supplies[0] if supplies else {}
        bundle_id = str(s0.get("bundle_id") or "").strip()
        if not bundle_id:
            return []

        raw = self.iter_bundle_items(bundle_id)

        items: List[Dict[str, Any]] = []
        for it in raw:
            offer_id = str(it.get("offer_id") or it.get("offerId") or "").strip()
            qty = it.get("quantity") or it.get("qty") or 0
            try:
                qty_f = float(qty)
            except Exception:
                qty_f = 0.0
            if offer_id and qty_f > 0:
                items.append({"offer_id": offer_id, "quantity": qty_f})

        return items
