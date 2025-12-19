from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from datetime import datetime, timezone

from app.http import request_json, HttpError

def ms_moment_from_iso(iso: str) -> str:
    s = (iso or "").strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s).astimezone(timezone.utc)
    # MoySklad moment отлично принимает с +0000
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "+0000"

def ms_meta(entity: str, uuid: str) -> Dict[str, Any]:
    return {
        "meta": {
            "href": f"https://api.moysklad.ru/api/remap/1.2/entity/{entity}/{uuid}",
            "type": entity,
            "mediaType": "application/json",
        }
    }


@dataclass(frozen=True)
class MsFboConfig:
    organization_id: str
    counterparty_ozon_id: str

    store_src_id: str  # СКЛАД
    store_fbo_id: str  # FBO

    state_customerorder_fbo_id: str
    state_move_supply_id: str
    state_demand_fbo_id: str

    sales_channel_id: str  # кабинет-зависимый

    # Политики
    set_move_external_code: bool = True  # важно для стабильного апдейта
    prices_without_vat: bool = True


class MoySkladSupplyService:
    def __init__(self, *, ms_token: str, cfg: MsFboConfig) -> None:
        self.ms_token = ms_token
        self.cfg = cfg
        self.base = "https://api.moysklad.ru/api/remap/1.2"

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.ms_token}",
            "Content-Type": "application/json",
            "Accept": "application/json;charset=utf-8",
        }

    # -------- Bundles --------

    def get_bundle(self, bundle_id: str) -> Dict[str, Any]:
        url = f"{self.base}/entity/bundle/{bundle_id}"
        return request_json("GET", url, headers=self._headers(), timeout=60)

    # -------- CustomerOrder --------

    def find_customerorder_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base}/entity/customerorder"
        params = {"filter": f"externalCode={external_code}"}
        rep = request_json("GET", url, headers=self._headers(), params=params, timeout=60)
        rows = rep.get("rows") or []
        return rows[0] if rows else None

    def create_customerorder(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base}/entity/customerorder"
        return request_json("POST", url, headers=self._headers(), json_body=payload, timeout=60)

    def update_customerorder(self, customerorder_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base}/entity/customerorder/{customerorder_id}"
        return request_json("PUT", url, headers=self._headers(), json_body=payload, timeout=60)

    def replace_customerorder_positions(self, customerorder_id: str, positions: List[Dict[str, Any]]) -> None:
        """
        В вашем МС POST позиций принимает МАССИВ (мы проверили).
        PUT на коллекцию позиций требует id, поэтому делаем: GET -> DELETE -> POST.
        """
        list_url = f"{self.base}/entity/customerorder/{customerorder_id}/positions"

        rep = request_json("GET", list_url, headers=self._headers(), params={"limit": 1000}, timeout=120)
        rows = rep.get("rows") or []

        for r in rows:
            pid = r.get("id")
            if not pid:
                continue
            del_url = f"{list_url}/{pid}"
            request_json("DELETE", del_url, headers=self._headers(), timeout=60)

        if positions:
            request_json("POST", list_url, headers=self._headers(), json_body=positions, timeout=120)

    def set_customerorder_unconducted(self, customerorder_id: str) -> None:
        self.update_customerorder(customerorder_id, {"applicable": False})

    def delete_customerorder(self, customerorder_id: str) -> None:
        url = f"{self.base}/entity/customerorder/{customerorder_id}"
        request_json("DELETE", url, headers=self._headers(), timeout=60)

    # -------- Demand --------

    def find_demand_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base}/entity/demand"
        params = {"filter": f"externalCode={external_code}"}
        rep = request_json("GET", url, headers=self._headers(), params=params, timeout=60)
        rows = rep.get("rows") or []
        return rows[0] if rows else None

    def create_demand_from_customerorder(
        self,
        customerorder_meta: Dict[str, Any],
        *,
        external_code: str,
        description: str,
    ) -> Dict[str, Any]:
        url = f"{self.base}/entity/demand"
        payload: Dict[str, Any] = {
            "externalCode": external_code,
            "customerOrder": customerorder_meta,  # {"meta": {...}}
            "organization": ms_meta("organization", self.cfg.organization_id),
            "agent": ms_meta("counterparty", self.cfg.counterparty_ozon_id),
            "store": ms_meta("store", self.cfg.store_fbo_id),
            "state": ms_meta("state", self.cfg.state_demand_fbo_id),
            "salesChannel": ms_meta("saleschannel", self.cfg.sales_channel_id),
            "description": description,
        }
        # можно увеличить timeout, если нужно (у тебя были таймауты)
        return request_json("POST", url, headers=self._headers(), json_body=payload, timeout=180)

    # -------- Move --------

    def find_move_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base}/entity/move"
        params = {"filter": f"externalCode={external_code}"}
        rep = request_json("GET", url, headers=self._headers(), params=params, timeout=60)
        rows = rep.get("rows") or []
        return rows[0] if rows else None

    def create_move(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base}/entity/move"
        return request_json("POST", url, headers=self._headers(), json_body=payload, timeout=60)

    def update_move(self, move_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base}/entity/move/{move_id}"
        return request_json("PUT", url, headers=self._headers(), json_body=payload, timeout=60)

    def replace_move_positions(self, move_id: str, positions: List[Dict[str, Any]]) -> None:
        list_url = f"{self.base}/entity/move/{move_id}/positions"

        rep = request_json("GET", list_url, headers=self._headers(), params={"limit": 1000}, timeout=120)
        rows = rep.get("rows") or []

        for r in rows:
            pid = r.get("id")
            if not pid:
                continue
            del_url = f"{list_url}/{pid}"
            request_json("DELETE", del_url, headers=self._headers(), timeout=60)

        if positions:
            request_json("POST", list_url, headers=self._headers(), json_body=positions, timeout=120)

    def delete_move(self, move_id: str) -> None:
        url = f"{self.base}/entity/move/{move_id}"
        request_json("DELETE", url, headers=self._headers(), timeout=60)

    # -------- Assortment + Price --------

    def find_assortment_by_article(self, article: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base}/entity/assortment"
        params = {"filter": f"article={article}"}
        rep = request_json("GET", url, headers=self._headers(), params=params, timeout=60)
        rows = rep.get("rows") or []
        return rows[0] if rows else None

    def get_sale_price_value(self, assortment: Dict[str, Any]) -> int:
        sale_prices = assortment.get("salePrices") or []
        if not sale_prices:
            return 0
        return int(sale_prices[0].get("value") or 0)

    # -------- High level --------

    def upsert_supply_customerorder_and_move(
        self,
        *,
        supply_number: str,
        shipment_planned_iso: str,
        description: str,
        items: List[Dict[str, Any]],  # items from Ozon bundle: offer_id, quantity
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        existing = self.find_customerorder_by_external_code(supply_number)

        order_payload: Dict[str, Any] = {
            "externalCode": supply_number,
            "name": supply_number,
            "organization": ms_meta("organization", self.cfg.organization_id),
            "agent": ms_meta("counterparty", self.cfg.counterparty_ozon_id),
            "store": ms_meta("store", self.cfg.store_fbo_id),
            "state": ms_meta("state", self.cfg.state_customerorder_fbo_id),
            "salesChannel": ms_meta("saleschannel", self.cfg.sales_channel_id),
            "shipmentPlannedMoment": ms_moment_from_iso(shipment_planned_iso),
            "description": description,
            "applicable": True,
        }

        if dry_run:
            co = existing or {"id": "DRYRUN", "meta": {"href": "DRYRUN"}}
        else:
            if existing:
                co = self.update_customerorder(existing["id"], order_payload)
            else:
                co = self.create_customerorder(order_payload)

        # ---- positions for CustomerOrder (bundles allowed) ----
        positions: List[Dict[str, Any]] = []
        for it in items:
            offer_id = str(it.get("offer_id") or "").strip()
            qty = float(it.get("quantity") or 0)
            if not offer_id or qty <= 0:
                continue

            a = self.find_assortment_by_article(offer_id)
            if not a:
                continue  # по ТЗ: если не найден — пропускаем

            price_value = self.get_sale_price_value(a)
            positions.append(
                {
                    "assortment": {"meta": a["meta"]},
                    "quantity": qty,
                    "price": price_value,
                }
            )

        if not dry_run:
            self.replace_customerorder_positions(co["id"], positions)

        # ---- Move ----
        move_key = supply_number
        if dry_run:
            return co

        move = self.find_move_by_external_code(move_key)
        move_payload: Dict[str, Any] = {
            "externalCode": move_key,
            "organization": ms_meta("organization", self.cfg.organization_id),
            "sourceStore": ms_meta("store", self.cfg.store_src_id),
            "targetStore": ms_meta("store", self.cfg.store_fbo_id),
            "state": ms_meta("state", self.cfg.state_move_supply_id),
            "description": description,
            "applicable": False,  # по умолчанию не проведено
        }

        try:
            mv = self.update_move(move["id"], move_payload) if move else self.create_move(move_payload)

            # Move: bundle запрещён — разворачиваем в компоненты
            move_positions: List[Dict[str, Any]] = []
            agg: Dict[str, float] = {}  # href -> qty

            for p in positions:
                a_meta = (p.get("assortment") or {}).get("meta")
                qty = float(p.get("quantity") or 0)
                if not a_meta or qty <= 0:
                    continue

                a_type = (a_meta.get("type") or "").lower()
                a_href = a_meta.get("href") or ""
                a_id = a_href.rstrip("/").split("/")[-1] if a_href else ""

                if a_type == "bundle" and a_id:
                    b = self.get_bundle(a_id)
                    comps = ((b.get("components") or {}).get("rows")) or []
                    for c in comps:
                        c_qty = float(c.get("quantity") or 0)
                        c_meta = ((c.get("assortment") or {}).get("meta")) or {}
                        c_href = c_meta.get("href")
                        if not c_href or c_qty <= 0:
                            continue
                        agg[c_href] = agg.get(c_href, 0.0) + qty * c_qty
                else:
                    agg[a_href] = agg.get(a_href, 0.0) + qty

            for href, q in agg.items():
                if q <= 0:
                    continue
                parts = href.rstrip("/").split("/")
                ent_type = parts[-2]
                move_positions.append(
                    {
                        "assortment": {
                            "meta": {
                                "href": href,
                                "type": ent_type,
                                "mediaType": "application/json",
                            }
                        },
                        "quantity": q,
                    }
                )

            # 3007: нет товара на складе → делаем move НЕ проведённым
            try:
                self.replace_move_positions(mv["id"], move_positions)
            except HttpError as e:
                txt = getattr(e, "body", None) or getattr(e, "text", None) or str(e)
                # 3007: нет товара на складе — просто оставляем перемещение непроведённым и идём дальше
                if e.status_code == 412 and "3007" in txt:
                    # можно оставить запись в лог
                    print(f"[move] {supply_number}: not enough stock -> move left applicable=false, positions not applied")
                else:
                    raise

        except Exception:
            # по ТЗ: если перемещение не удается создать/обновить — снимаем проведение заказа
            self.set_customerorder_unconducted(co["id"])
            raise

        return co
