from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from app.http import request_json, HttpError


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

    sales_channel_id: str  # дефолт, но мы меняем на кабинетный через set_sales_channel()


class MoySkladSupplyService:
    def __init__(self, *, ms_token: str, cfg: MsFboConfig) -> None:
        self.ms_token = ms_token
        self.cfg = cfg
        self.base = "https://api.moysklad.ru/api/remap/1.2"
        self._assort_cache: Dict[str, Optional[Dict[str, Any]]] = {}

    def set_sales_channel(self, sales_channel_id: str) -> None:
        # для каждого кабинета свой канал продаж
        self.cfg = MsFboConfig(
            organization_id=self.cfg.organization_id,
            counterparty_ozon_id=self.cfg.counterparty_ozon_id,
            store_src_id=self.cfg.store_src_id,
            store_fbo_id=self.cfg.store_fbo_id,
            state_customerorder_fbo_id=self.cfg.state_customerorder_fbo_id,
            state_move_supply_id=self.cfg.state_move_supply_id,
            state_demand_fbo_id=self.cfg.state_demand_fbo_id,
            sales_channel_id=sales_channel_id,
        )

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.ms_token}",
            "Content-Type": "application/json",
            "Accept": "application/json;charset=utf-8",
        }

    # ---------- helper: comment (НЕ обновляем, только ставим при создании) ----------

    def _build_comment_once(self, order_number: str, core: Dict[str, Any]) -> str:
        # <номер> - <склад получатель>
        wh_name = ((core.get("drop_off_warehouse") or {}).get("name")) or ""
        wh_name = (wh_name or "").strip()
        if wh_name:
            return f"{order_number} - {wh_name}"
        return str(order_number)

    # ---------- assortment ----------

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
        sale_prices = assortment.get("salePrices") or []
        if not sale_prices:
            return 0
        for sp in sale_prices:
            pt = sp.get("priceType") or {}
            name = (pt.get("name") or "").strip().lower()
            if name in ("цена продажи", "sale price"):
                return int(sp.get("value") or 0)
        return int(sale_prices[0].get("value") or 0)

    # ---------- customerorder ----------

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
        list_url = f"{self.base}/entity/customerorder/{customerorder_id}/positions"

        rep = request_json("GET", list_url, headers=self._headers(), params={"limit": 1000}, timeout=120)
        rows = rep.get("rows") or []
        for r in rows:
            pid = r.get("id")
            if pid:
                request_json("DELETE", f"{list_url}/{pid}", headers=self._headers(), timeout=60)

        if positions:
            # В вашем МС корректно работает POST массива позиций
            request_json("POST", list_url, headers=self._headers(), json_body=positions, timeout=120)

    def delete_customerorder(self, customerorder_id: str) -> None:
        url = f"{self.base}/entity/customerorder/{customerorder_id}"
        request_json("DELETE", url, headers=self._headers(), timeout=60)

    def upsert_customerorder(
        self,
        *,
        order_number: str,
        shipment_planned_moment: str,  # уже в формате МС
        core: Dict[str, Any],
        items: List[Dict[str, Any]],
        dry_run: bool,
    ) -> Dict[str, Any]:
        existing = self.find_customerorder_by_external_code(order_number)

        payload: Dict[str, Any] = {
            "externalCode": order_number,
            "name": order_number,
            "organization": ms_meta("organization", self.cfg.organization_id),
            "agent": ms_meta("counterparty", self.cfg.counterparty_ozon_id),
            "store": ms_meta("store", self.cfg.store_fbo_id),
            "state": ms_meta("state", self.cfg.state_customerorder_fbo_id),
            "salesChannel": ms_meta("saleschannel", self.cfg.sales_channel_id),
            "applicable": True,  # ЗАКАЗ ВСЕГДА ПРОВЕДЕН
        }

        # shipmentPlannedMoment обновляем при изменениях (просто ставим всегда)
        if shipment_planned_moment:
            payload["shipmentPlannedMoment"] = shipment_planned_moment

        if dry_run:
            return existing or {"id": "DRYRUN", "meta": {"href": "DRYRUN"}}

        if existing:
            # description НЕ обновляем (по ТЗ)
            co = self.update_customerorder(existing["id"], payload)
        else:
            # description ставим только при создании
            payload["description"] = self._build_comment_once(order_number, core)
            co = self.create_customerorder(payload)

        # позиции пересобираем полностью (если demand нет — это гарантируется снаружи)
        positions: List[Dict[str, Any]] = []
        for it in items or []:
            offer_id = str(it.get("offer_id") or "").strip()
            qty = float(it.get("quantity") or 0)
            if not offer_id or qty <= 0:
                continue
            a = self.find_assortment_by_article(offer_id)
            if not a:
                continue
            price = self._pick_sale_price_value(a)
            positions.append({"assortment": {"meta": a["meta"]}, "quantity": qty, "price": price})

        self.replace_customerorder_positions(co["id"], positions)
        return co

    # ---------- demand ----------

    def find_demand_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base}/entity/demand"
        params = {"filter": f"externalCode={external_code}"}
        rep = request_json("GET", url, headers=self._headers(), params=params, timeout=60)
        rows = rep.get("rows") or []
        return rows[0] if rows else None

    def create_demand(
        self,
        *,
        customerorder_meta: Dict[str, Any],
        order_number: str,
        core: Dict[str, Any],
    ) -> Dict[str, Any]:
        url = f"{self.base}/entity/demand"
        payload: Dict[str, Any] = {
            "externalCode": order_number,  # только для поиска, номер (name) оставит МС по хронологии
            "customerOrder": customerorder_meta,
            "organization": ms_meta("organization", self.cfg.organization_id),
            "agent": ms_meta("counterparty", self.cfg.counterparty_ozon_id),
            "store": ms_meta("store", self.cfg.store_fbo_id),
            "state": ms_meta("state", self.cfg.state_demand_fbo_id),
            "salesChannel": ms_meta("saleschannel", self.cfg.sales_channel_id),
            "description": self._build_comment_once(order_number, core),  # при создании
        }
        return request_json("POST", url, headers=self._headers(), json_body=payload, timeout=180)

    def ensure_demand_from_customerorder(
        self,
        *,
        order_number: str,
        customerorder_meta: Dict[str, Any],
        core: Dict[str, Any],
        dry_run: bool,
    ) -> Optional[Dict[str, Any]]:
        if dry_run:
            return None
        d = self.find_demand_by_external_code(order_number)
        if d:
            return d
        return self.create_demand(customerorder_meta=customerorder_meta, order_number=order_number, core=core)

    # ---------- move ----------

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
            if pid:
                request_json("DELETE", f"{list_url}/{pid}", headers=self._headers(), timeout=60)

        if positions:
            request_json("POST", list_url, headers=self._headers(), json_body=positions, timeout=120)

    def delete_move(self, move_id: str) -> None:
        url = f"{self.base}/entity/move/{move_id}"
        request_json("DELETE", url, headers=self._headers(), timeout=60)

    def get_bundle(self, bundle_id: str) -> Dict[str, Any]:
        url = f"{self.base}/entity/bundle/{bundle_id}"
        return request_json("GET", url, headers=self._headers(), timeout=60)

    def _build_move_positions_from_items(self, items: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], bool]:
        """
        В move нельзя bundle. Поэтому:
        - находим ассортимент по артикулу
        - если bundle -> разворачиваем компоненты
        Возвращаем (positions, has_positions)
        """
        agg: Dict[str, float] = {}

        for it in items or []:
            offer_id = str(it.get("offer_id") or "").strip()
            qty = float(it.get("quantity") or 0)
            if not offer_id or qty <= 0:
                continue

            a = self.find_assortment_by_article(offer_id)
            if not a:
                continue

            meta = a.get("meta") or {}
            a_type = str(meta.get("type") or "").lower()
            href = str(meta.get("href") or "")
            a_id = href.rstrip("/").split("/")[-1] if href else ""

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
                if href:
                    agg[href] = agg.get(href, 0.0) + qty

        positions: List[Dict[str, Any]] = []
        for href, q in agg.items():
            if q <= 0:
                continue
            parts = href.rstrip("/").split("/")
            ent_type = parts[-2]
            positions.append(
                {
                    "assortment": {"meta": {"href": href, "type": ent_type, "mediaType": "application/json"}},
                    "quantity": q,
                }
            )

        return positions, bool(positions)

    def upsert_move_linked_to_customerorder(
        self,
        *,
        order_number: str,
        customerorder_meta: Dict[str, Any],
        core: Dict[str, Any],
        items: List[Dict[str, Any]],
        dry_run: bool,
    ) -> Tuple[Optional[Dict[str, Any]], bool]:
        """
        Move создаём/обновляем (если demand нет — проверяется снаружи).
        - Связь с заказом: customerOrder (чтобы было в "Связанных документах")
        - Проводим move, но если 3007 -> оставляем непроведённым.
        Возвращаем (move, has_positions)
        """
        if dry_run:
            return None, False

        existing = self.find_move_by_external_code(order_number)

        # description не обновляем — только при создании
        payload_base: Dict[str, Any] = {
            "externalCode": order_number,
            "organization": ms_meta("organization", self.cfg.organization_id),
            "sourceStore": ms_meta("store", self.cfg.store_src_id),
            "targetStore": ms_meta("store", self.cfg.store_fbo_id),
            "state": ms_meta("state", self.cfg.state_move_supply_id),
            "customerOrder": customerorder_meta,  # связь "как в UI"
            "applicable": False,  # сначала непроведённое
        }

        if existing:
            mv = self.update_move(existing["id"], payload_base)
        else:
            payload_base["description"] = self._build_comment_once(order_number, core)
            mv = self.create_move(payload_base)

        positions, has_positions = self._build_move_positions_from_items(items)
        # пересобираем позиции полностью
        self.replace_move_positions(mv["id"], positions)

        # пытаемся провести ТОЛЬКО если есть позиции
        if has_positions:
            try:
                mv = self.update_move(mv["id"], {"applicable": True})
            except HttpError as e:
                # 3007: нет товара на складе -> оставляем непроведённым и не падаем
                txt = getattr(e, "text", "") or str(e)
                if e.status_code == 412 and ("3007" in txt or "Нельзя переместить товар" in txt):
                    self.update_move(mv["id"], {"applicable": False})
                else:
                    raise

        return mv, has_positions
