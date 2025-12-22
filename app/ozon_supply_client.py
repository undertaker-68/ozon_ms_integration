import requests

class OzonSupplyClient:
    def __init__(self, cabinet, base_url="https://api-seller.ozon.ru"):
        self.cabinet = cabinet
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "Client-Id": cabinet.client_id,
            "Api-Key": cabinet.api_key,
        })

    def supply_order_items(self, order_id: int) -> list[dict]:
        url = f"{self.base_url}/v3/supply-order/items"
        payload = {"order_id": order_id}
        r = self.session.post(url, json=payload, timeout=60)
        r.raise_for_status()
        return (r.json() or {}).get("items") or []

    def iter_supply_orders_full(self, states: list) -> list[dict]:
        url = f"{self.base_url}/v3/supply-order/list"
        params = {"filter": ",".join(states), "limit": 100}
        r = self.session.post(url, json=params, timeout=60)
        r.raise_for_status()
        return r.json().get("orders", [])
