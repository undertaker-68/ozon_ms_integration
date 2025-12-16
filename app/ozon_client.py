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
        self.cache_path = os.path.join(cache_dir, f"offer_ids_{creds.name.lower()}.json")

    def _headers(self) -> Dict[str, str]:
        # DEBUG (временно): проверяем, что реально уходит
        # печатаем только длину ключа и последние 4 символа
        def _headers(self) -> Dict[str, str]:
        h = {
            "Client-Id": self.creds.client_id,
            "Api-Key": self.creds.api_key,
            "Content-Type": "application/json",
        }
        return h
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

        url = f"{OZON_BASE}/v3/product/list"
        offer_ids: Set[str] = set()

        last_id = ""
        limit = 1000

        while True:
            body: Dict[str, Any] = {"filter": {}, "last_id": last_id, "limit": limit}
            data = request_json("POST", url, headers=self._headers(), json_body=body, timeout=60)

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
                json.dump({"ts": now, "offer_ids": sorted(offer_ids)}, f, ensure_ascii=False)
        except Exception:
            pass

        return offer_ids

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
        return request_json("POST", url, headers=self._headers(), json_body=payload, timeout=60)
