from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

DEFAULT_BASE_URL = "https://api-seller.ozon.ru"


@dataclass(frozen=True)
class OzonCabinet:
    name: str
    client_id: str
    api_key: str
    base_url: str = DEFAULT_BASE_URL


class OzonSupplyClient:
    def __init__(self, cabinet: OzonCabinet) -> None:
        self.cabinet = cabinet

    def _headers(self) -> Dict[str, str]:
        return {
            "Client-Id": str(self.cabinet.client_id),
            "Api-Key": str(self.cabinet.api_key),
            "Content-Type": "application/json; charset=utf-8",
        }

    def supply_order_list(
        self,
        *,
        states: List[str],
        limit: int = 100,
        last_id: str = "",
        sort_by: str = "ORDER_CREATION",
        sort_dir: str = "DESC",
    ) -> Dict[str, Any]:
        url = f"{self.cabinet.base_url}/v3/supply-order/list"
        payload = {
            "filter": {"states": states},
            "limit": limit,
            "sort_by": sort_by,
            "sort_dir": sort_dir,
            "last_id": last_id,
        }
        r = requests.post(url, headers=self._headers(), json=payload, timeout=60)
        r.raise_for_status()
        return r.json()

    def supply_order_get(self, *, order_ids: List[int]) -> Dict[str, Any]:
        url = f"{self.cabinet.base_url}/v3/supply-order/get"
        payload = {"order_ids": order_ids}
        r = requests.post(url, headers=self._headers(), json=payload, timeout=60)
        r.raise_for_status()
        return r.json()

    def supply_order_bundle(self, *, bundle_ids: List[str], limit: int = 100, last_id: str = "") -> Dict[str, Any]:
        url = f"{self.cabinet.base_url}/v3/supply-order/bundle"
        payload = {"bundle_ids": bundle_ids, "limit": limit, "last_id": last_id}
        r = requests.post(url, headers=self._headers(), json=payload, timeout=60)
        r.raise_for_status()
        return r.json()

    def iter_supply_orders_full(self, *, states: List[str], limit: int = 100, batch_get: int = 50) -> List[Dict[str, Any]]:
        """
        Возвращает list orders из /v3/supply-order/get с заполненным order_number/state/timeslot/supplies...
        """
        out: List[Dict[str, Any]] = []
        last_id = ""
        while True:
            lst = self.supply_order_list(states=states, limit=limit, last_id=last_id)
            order_ids = lst.get("order_ids") or []
            last_id = lst.get("last_id") or ""
            if not order_ids:
                break

            for i in range(0, len(order_ids), batch_get):
                chunk = order_ids[i : i + batch_get]
                rep = self.supply_order_get(order_ids=chunk)
                out.extend(rep.get("orders") or [])

            if not last_id:
                break
        return out

    def iter_bundle_items(self, *, bundle_id: str) -> List[Dict[str, Any]]:
        """
        Возвращает items вида {"offer_id": "...", "quantity": N}
        """
        if not bundle_id:
            return []
        last_id = ""
        items: List[Dict[str, Any]] = []
        while True:
            rep = self.supply_order_bundle(bundle_ids=[bundle_id], limit=100, last_id=last_id)
            bundles = rep.get("bundles") or []
            if bundles:
                b0 = bundles[0] or {}
                for it in (b0.get("items") or []):
                    offer_id = str(it.get("offer_id") or "").strip()
                    qty = it.get("quantity")
                    if offer_id and qty:
                        items.append({"offer_id": offer_id, "quantity": qty})

            last_id = rep.get("last_id") or ""
            if not last_id:
                break
        return items

    def get_supply_order_items(self, order: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Правильный источник товаров поставки:
        order.supplies[0].bundle_id -> /v3/supply-order/bundle -> items(offer_id, quantity)
        """
        supplies = order.get("supplies") or []
        s0 = supplies[0] if supplies else {}
        bundle_id = str(s0.get("bundle_id") or "").strip()
        if not bundle_id:
            return []
        return self.iter_bundle_items(bundle_id=bundle_id)
