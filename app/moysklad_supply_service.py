from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from datetime import datetime, timezone

from app.http import request_json, HttpError


def ms_moment_from_iso(iso: str) -> str:
    s = (iso or "").strip()
    if not s:
        return ""
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s).astimezone(timezone.utc)
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
    set_move_external_code: bool = True
    prices_without_vat: bool = True


class MoySkladSupplyService:
    def __init__(self, *, ms_token: str, cfg: MsFboConfig) -> None:
        self.ms_token = ms_token
        self.cfg = cfg
        self.base = "https://api.moysklad.ru/api/remap/1.2"

        # cache: article -> (assortment_dict or None)
        self._assort_cache: Dict[str, Optional[Dict[str, Any]]] = {}

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
        В вашем МС POST позиций принимает МАССИВ.
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
            "customerOrder": customerorder_meta,
            "organization": ms_meta("organization", self.cfg.organization_id),
            "agent": ms_meta("counterparty", self.cfg.counterparty_ozon_id),
            "store": ms_meta("store", self.cfg.store_fbo_id),
            "state": ms_meta("state", self.cfg.state_demand_fbo_id),
            "salesChannel": ms_meta("saleschannel", self.cfg.sales_channel_id),
            "description": description,
        }
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
        article = (article or "").strip()
        if not article:
            return None

        if article in self._assort_cache:
            return self._assort_cache[article]

        url = f"{self.base}/entity/assortment"
        params = {"filter": f"article={article}"}
        rep = request_json("GET", url, headers=self._headers(), params=params, timeout=60)
        rows = rep.get("rows") or []
        a = rows[0] if rows else None
        self._assort_cache[article] = a
        return a

    def _pick_sale_price_value(self, assortment: Dict[str, Any]) -> int:
        """
        Берём 'Цена продажи' если найдём по имени, иначе первый salePrices.
        """
        sale_prices = assortment.get("salePrices") or []
        if not sale_prices:
            return 0

        for sp in sale_prices:
            pt = sp.get("priceType") or {}
            name = (pt.get("name") or "").strip().lower()
            if name == "цена продажи" or name == "sale price":
                return int(sp.get("value") or 0)

        return int(sale_prices[0].get("value") or 0)

    # -------- High level --------

    def upsert_supply_customerorder_and_move(
        self,
        *,
        supply_number: str,
        shipment_planned_iso: str,
        description: str,
        items: List[Dict[str, Any]],
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Гарантии:
        - CustomerOrder: externalCode = supply_number, name = supply_number
        - CustomerOrder.shipmentPlannedMoment проставляем корректным форматом (если дата есть)
        - Move создаём ВСЕГДА (даже если позиций нет / товары не найдены)
        - Если что-то ломается на позициях — снимаем applicable у заказа (по ТЗ)
        """
        supply_number = str(supply_number or "").strip()
        existing = self.find_customerorder_by_external_code(supply_number)

        shipment_moment = ms_moment_from_iso(shipment_planned_iso)

        order_payload: Dict[str, Any] = {
            "externalCode": supply_number,
            "name": supply_number,  # ВАЖНО: номер заказа в МС = номер поставки
            "organization": ms_meta("organization", self.cfg.organization_id),
            "agent": ms_meta("counterparty", self.cfg.counterparty_ozon_id),
            "store": ms_meta("store", self.cfg.store_fbo_id),
            "state": ms_meta("state", self.cfg.state_customerorder_fbo_id),
            "salesChannel": ms_meta("saleschannel", self.cfg.sales_channel_id),
            "description": description,
            "applicable": True,
        }
        if shipment_moment:
            order_payload["shipmentPlannedMoment"] = shipment_moment

        if dry_run:
            co = existing or {"id": "DRYRUN", "meta": {"href": "DRYRUN"}}
            return co

        # 1) Сначала создаём/обновляем заказ (без позиций)
        if existing:
            co = self.update_customerorder(existing["id"], order_payload)
        else:
            co = self.create_customerorder(order_payload)

        # 2) Сразу создаём/обновляем Move (чтобы он был даже если позиции не запишутся)
        move_key = supply_number
        move = self.find_move_by_external_code(move_key)

        move_payload: Dict[str, Any] = {
            "externalCode": move_key,
            "organization": ms_meta("organization", self.cfg.organization_id),
            "sourceStore": ms_meta("store", self.cfg.store_src_id),
            "targetStore": ms_meta("store", self.cfg.store_fbo_id),
            "state": ms_meta("state", self.cfg.state_move_supply_id),
            "description": description,
            "applicable": False,  # всегда НЕ проведено, чтобы не упираться в остатки
        }

        mv = self.update_move(move["id"], move_payload) if move else self.create_move(move_payload)

        # 3) Теперь собираем позиции. Если на этом этапе будет ошибка — Move уже существует.
        try:
            # ---- positions for CustomerOrder (bundles allowed) ----
            positions: List[Dict[str, Any]] = []
            for it in items:
                offer_id = str(it.get("offer_id") or "").strip()
                qty = float(it.get("quantity") or 0)
                if not offer_id or qty <= 0:
                    continue

                a = self.find_assortment_by_article(offer_id)
                if not a:
                    continue  # по ТЗ: если не найден — пропуск

                price_value = self._pick_sale_price_value(a)
                positions.append(
                    {
                        "assortment": {"meta": a["meta"]},
                        "quantity": qty,
                        "price": price_value,
                    }
                )

            self.replace_customerorder_positions(co["id"], positions)

            # ---- Move positions (bundle запрещён — разворачиваем в компоненты) ----
            # Если позиций нет — просто оставим Move пустым (но он уже создан)
            agg: Dict[str, float] = {}
            for p in positions:
                a_meta = (p.get("assortment") or {}).get("meta") or {}
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
                    if a_href:
                        agg[a_href] = agg.get(a_href, 0.0) + qty

            move_positions: List[Dict[str, Any]] = []
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

            try:
                self.replace_move_positions(mv["id"], move_positions)
            except HttpError as e:
                txt = getattr(e, "body", None) or getattr(e, "text", None) or str(e)
                if e.status_code == 412 and "3007" in txt:
                    # нет товара на складе — оставляем move applicable=false и не падаем
                    print(f"[move] {supply_number}: not enough stock -> move left applicable=false, positions not applied")
                else:
                    raise

        except Exception:
            # по ТЗ: если перемещение/позиции не удаётся создать — снимаем проведение заказа
            self.set_customerorder_unconducted(co["id"])
            raise

        return co
