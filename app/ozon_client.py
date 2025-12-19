from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Set
import json
import os
import time

from .http import request_json

OZON_BASE = "https://api-seller.ozon.ru"

@dataclass(frozen=True)
class OzonCreds:
    name: str
    client_id: str
    api_key: str
    warehouse_id: int

class OzonClient:
    def __init__(self, creds: OzonCreds, cache_dir: str):
        self.creds = creds
        os.makedirs(cache_dir, exist_ok=True)
        self.cache_path = os.path.join(
            cache_dir, f"offer_ids_{creds.name.lower()}.json"
        )

    def _headers(self) -> Dict[str, str]:
        return {
            "Client-Id": self.creds.client_id,
            "Api-Key": self.creds.api_key,
            "Content-Type": "application/json",
        }

    def list_offer_ids(self, ttl_seconds: int = 7 * 60) -> Set[str]:
        now = time.time()

        # cache
        try:
            if os.path.exists(self.cache_path):
                with open(self.cache_path, "r", encoding="utf-8") as f:
                    c = json.load(f)
                if (now - float(c.get("ts", 0))) < ttl_seconds:
                    return set(c.get("offer_ids", []))
        except Exception:
            pass

        offer_ids: Set[str] = set()

        last_id = ""
        limit = 100  # безопасный лимит
        seen_last_ids = set()

        url = f"{OZON_BASE}/v3/product/list"

        while True:
            if last_id in seen_last_ids:
                break
            seen_last_ids.add(last_id)

            body: Dict[str, Any] = {
                "filter": {},
                "last_id": last_id,
                "limit": limit,
            }

            data = request_json(
                "POST",
                url,
                headers=self._headers(),
                json_body=body,
                timeout=60,
            )

            result = data.get("result") or {}
            items = result.get("items") or []

            for it in items:
                oid = it.get("offer_id")
                if oid:
                    offer_ids.add(str(oid))

            last_id = str(result.get("last_id") or "")
            if not items or not last_id:
                break

        try:
            with open(self.cache_path, "w", encoding="utf-8") as f:
                json.dump(
                    {"ts": now, "offer_ids": sorted(offer_ids)},
                    f,
                    ensure_ascii=False,
                )
        except Exception:
            pass

        return offer_ids

    # ---------------------------
    # FBO Supply Orders (v3)
    # ---------------------------

    def list_supply_order_ids(self, states: List[str], limit: int = 100) -> List[int]:
        """
        Returns list of supply order IDs (order_id) by states.
        Uses POST /v3/supply-order/list with pagination by last_id.
        """
        url = f"{OZON_BASE}/v3/supply-order/list"

        last_id = ""
        out: List[int] = []
        seen_last_ids = set()

        while True:
            if last_id in seen_last_ids:
                break
            seen_last_ids.add(last_id)

            body: Dict[str, Any] = {
                "filter": {"states": states},
                "limit": int(limit),
                "sort_by": "ORDER_CREATION",
                "sort_dir": "DESC",
                "last_id": last_id,
            }

            data = request_json(
                "POST", url,
                headers=self._headers(),
                json_body=body,
                timeout=60,
            )

            ids = data.get("order_ids") or []
            for x in ids:
                try:
                    out.append(int(x))
                except Exception:
                    pass

            last_id = str(data.get("last_id") or "")
            if not ids or not last_id:
                break

        return out

    def get_supply_order(self, order_id: int) -> Dict[str, Any]:
        """
        Returns supply order core info.
        Uses POST /v3/supply-order/get
        We return the first order dict from response.
        """
        url = f"{OZON_BASE}/v3/supply-order/get"
        body = {"order_id": int(order_id)}

        data = request_json(
            "POST", url,
            headers=self._headers(),
            json_body=body,
            timeout=60,
        )

        # your example: {"orders":[{...}]}
        orders = data.get("orders") or []
        if not orders:
            return {}
        return orders[0]

    def get_supply_order_items(self, order_id: int) -> List[Dict[str, Any]]:
        """
        Returns list of items for supply order in normalized format:
          [{"offer_id": "...", "quantity": N}, ...]

        Primary: POST /v3/supply-order/items
        Fallback: try to extract from get_supply_order() payload if items are embedded.
        """
        # 1) Try dedicated items endpoint
        url = f"{OZON_BASE}/v3/supply-order/items"
        body = {"order_id": int(order_id), "limit": 1000, "offset": 0}

        try:
            data = request_json(
                "POST", url,
                headers=self._headers(),
                json_body=body,
                timeout=60,
            )

            # common shapes:
            # {"items":[...]} OR {"result":{"items":[...]}}
            items = data.get("items")
            if items is None:
                items = (data.get("result") or {}).get("items")

            out: List[Dict[str, Any]] = []
            for it in items or []:
                # try common fields
                offer_id = it.get("offer_id") or it.get("offerId") or it.get("offerID")
                qty = it.get("quantity") or it.get("qty") or it.get("count")
                if offer_id and qty:
                    out.append({"offer_id": str(offer_id), "quantity": float(qty)})
            if out:
                return out
        except Exception:
            pass

        # 2) Fallback: sometimes items are embedded in order/get response
        core = self.get_supply_order(order_id) or {}
        candidates = (
            core.get("items")
            or (core.get("result") or {}).get("items")
            or (core.get("order_items") or [])
        )

        out2: List[Dict[str, Any]] = []
        for it in candidates or []:
            offer_id = it.get("offer_id") or it.get("offerId") or it.get("offerID")
            qty = it.get("quantity") or it.get("qty") or it.get("count")
            if offer_id and qty:
                out2.append({"offer_id": str(offer_id), "quantity": float(qty)})

        return out2
    
    def set_stocks(self, stocks: List[Dict[str, Any]]) -> Dict[str, Any]:
        url = f"{OZON_BASE}/v2/products/stocks"
        payload = {
            "stocks": [
                {
                    "offer_id": s["offer_id"],
                    "warehouse_id": self.creds.warehouse_id,
                    "stock": int(s["stock"]),
                }
                for s in stocks
            ]
        }
        return request_json(
            "POST",
            url,
            headers=self._headers(),
            json_body=payload,
            timeout=60,
        )

    def _to_ozon_ts(self, dt: Any) -> str:
        """
        Convert datetime/str to Ozon ISO8601 string with Z.
        Accepts:
          - datetime with/without tzinfo
          - already ISO string
        """
        if dt is None:
            raise ValueError("datetime is required")
        if isinstance(dt, str):
            return dt
        # assume datetime
        try:
            import datetime as _dt
            if isinstance(dt, _dt.datetime):
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=_dt.timezone.utc)
                s = dt.astimezone(_dt.timezone.utc).isoformat()
                # Ozon examples use Z
                return s.replace("+00:00", "Z")
        except Exception:
            pass
        raise TypeError(f"Unsupported datetime type: {type(dt)}")

    def fbs_list(
        self,
        date_from: Any,
        date_to: Any,
        statuses: List[str] | None = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Returns list of postings (short) for FBS.
        Uses /v3/posting/fbs/list with offset/limit pagination.
        """
        url = f"{OZON_BASE}/v3/posting/fbs/list"

        df = self._to_ozon_ts(date_from)
        dt = self._to_ozon_ts(date_to)

        offset = 0
        out: List[Dict[str, Any]] = []

        while True:
            flt: Dict[str, Any] = {
                "since": df,
                "to": dt,
            }
            if statuses:
                flt["status"] = statuses

            body: Dict[str, Any] = {
                "filter": flt,
                "limit": int(limit),
                "offset": int(offset),
                # with/analytics/financial_data включаем только если реально нужно
                # "with": {"analytics_data": True, "financial_data": True},
            }

            data = request_json(
                "POST",
                url,
                headers=self._headers(),
                json_body=body,
                timeout=60,
            )

            result = data.get("result") or {}
            postings = result.get("postings") or []
            if postings:
                out.extend(postings)

            # Если вернулось меньше лимита — конец
            if not postings or len(postings) < int(limit):
                break

            offset += int(limit)

        return out

    def fbs_get(self, posting_number: str) -> Dict[str, Any]:
        """
        Returns full posting details for a single FBS posting.
        Uses /v3/posting/fbs/get
        """
        url = f"{OZON_BASE}/v3/posting/fbs/get"
        body = {
            "posting_number": posting_number,
            # включаем всё полезное: товары/аналитика/финансы
            "with": {
                "analytics_data": True,
                "barcodes": False,
                "financial_data": True,
                "translit": False,
            },
        }

        return request_json(
            "POST",
            url,
            headers=self._headers(),
            json_body=body,
            timeout=60,
        )
