from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

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

    store_src_id: str  # СКЛАД-источник (ваш)
    store_fbo_id: str  # FBO (ваш)

    state_customerorder_fbo_id: str
    state_move_supply_id: str
    state_demand_fbo_id: str

    sales_channel_id: str  # кабинет-зависимый


class MoySkladSupplyService:
    def __init__(self, *, ms_token: str, cfg: MsFboConfig) -> None:
        self.ms_token = ms_token
        self.cfg = cfg
        self.base = "https://api.moysklad.ru/api/remap/1.2"

        # caches
        self._assort_by_article_cache: Dict[str, Optional[Dict[str, Any]]] = {}
        self._assort_by_href_cache: Dict[str, Optional[Dict[str, Any]]] = {}
        self._bundle_cache: Dict[str, Optional[Dict[str, Any]]] = {}

    def set_sales_channel(self, sales_channel_id: str) -> None:
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

    # ---------- helper: comment (ставим только при создании заказа) ----------

    def _build_comment_once(self, order_number: str, core: Dict[str, Any]) -> str:
        """
        Комментарий: <номер> - <склад назначения>
        Склад назначения берем из supplies[0].storage_warehouse.name
        """
        supplies = core.get("supplies") or []
        s0 = supplies[0] if supplies else {}
        st = (s0.get("storage_warehouse") or {})
        wh_name = (st.get("name") or "").strip()
        if wh_name:
            return f"{order_number} - {wh_name}"
        return str(order_number)

    # ---------- assortment ----------

    def find_assortment_by_article(self, article: str) -> Optional[Dict[str, Any]]:
        article = (article or "").strip()
        if not article:
            return None
        if article in self._assort_by_article_cache:
            return self._assort_by_article_cache[article]

        url = f"{self.base}/entity/assortment"
        params = {"filter": f"article={article}"}
        rep = request_json("GET", url, headers=self._headers(), params=params, timeout=60)
        rows = rep.get("rows") or []
        a = rows[0] if rows else None
        self._assort_by_article_cache[article] = a
        return a

    def get_assortment_by_href(self, href: str) -> Optional[Dict[str, Any]]:
        """
        Нужен для компонентов комплектов (bundle->components->assortment.meta.href),
        чтобы достать цены/мету без гаданий.
        """
        href = (href or "").strip()
        if not href:
            return None
        if href in self._assort_by_href_cache:
            return self._assort_by_href_cache[href]
        try:
            a = request_json("GET", href, headers=self._headers(), timeout=60)
        except Exception:
            a = None
        self._assort_by_href_cache[href] = a
        return a

    def _pick_sale_price_value(self, assortment: Dict[str, Any]) -> int:
        """
        Цена в МС в копейках (int).
        Берем salePrices[0].value если есть, иначе 0.
        """
        sp = assortment.get("salePrices") or []
        if sp:
            v = sp[0].get("value")
            if isinstance(v, int):
                return v
            try:
                return int(float(v))
            except Exception:
                return 0
        return 0

    # ---------- bundle (комплект) ----------

    def get_bundle(self, bundle_id: str) -> Optional[Dict[str, Any]]:
        bundle_id = (bundle_id or "").strip()
        if not bundle_id:
            return None
        if bundle_id in self._bundle_cache:
            return self._bundle_cache[bundle_id]
        url = f"{self.base}/entity/bundle/{bundle_id}"
        b = request_json("GET", url, headers=self._headers(), timeout=60)
        self._bundle_cache[bundle_id] = b
        return b

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

    def get_customerorder_positions(self, customerorder_id: str) -> List[Dict[str, Any]]:
        url = f"{self.base}/entity/customerorder/{customerorder_id}/positions"
        rep = request_json("GET", url, headers=self._headers(), params={"limit": 1000}, timeout=120)
        return rep.get("rows") or []

    def delete_customerorder(self, customerorder_id: str) -> None:
        url = f"{self.base}/entity/customerorder/{customerorder_id}"
        request_json("DELETE", url, headers=self._headers(), timeout=60)

    def upsert_customerorder(
        self,
        *,
        order_number: str,
        shipment_planned_moment: str,
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
            "applicable": True,  # Заказ ВСЕГДА проведен
        }

        if shipment_planned_moment:
            payload["shipmentPlannedMoment"] = shipment_planned_moment

        if dry_run:
            return existing or {"id": "DRYRUN", "meta": {"href": "DRYRUN"}}

        if existing:
            # description НЕ обновляем
            co = self.update_customerorder(existing["id"], payload)
        else:
            payload["description"] = self._build_comment_once(order_number, core)
            co = self.create_customerorder(payload)

        # позиции пересобираем полностью (если demand нет — гарантируется снаружи)
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

    def _expand_items_to_move_positions(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Позиции Move:
        - обычные товары: assortment + qty + price (sale price)
        - bundle: раскрываем в компоненты (bundle.components.rows),
                 для каждого компонента: assortment meta + qty*component_qty + price (sale price компонента)
        """
        agg: Dict[str, float] = {}      # href -> qty
        meta_by_href: Dict[str, Dict[str, Any]] = {}  # href -> meta
        price_by_href: Dict[str, int] = {}            # href -> price

        def _remember(href: str, meta: Dict[str, Any], qty: float, price: int) -> None:
            if not href:
                return
            agg[href] = agg.get(href, 0.0) + qty
            if href not in meta_by_href and meta:
                meta_by_href[href] = meta
            if href not in price_by_href:
                price_by_href[href] = int(price or 0)

        for it in items or []:
            offer_id = str(it.get("offer_id") or "").strip()
            qty = float(it.get("quantity") or 0)
            if not offer_id or qty <= 0:
                continue

            a = self.find_assortment_by_article(offer_id)
            if not a:
                continue

            a_meta = (a.get("meta") or {})
            a_type = (a_meta.get("type") or "").lower()
            a_href = a_meta.get("href") or ""

            if a_type == "bundle" and a_href:
                bundle_id = a_href.rstrip("/").split("/")[-1]
                b = self.get_bundle(bundle_id)
                comps = ((b or {}).get("components") or {}).get("rows") or []
                for c in comps:
                    c_qty = float(c.get("quantity") or 0)
                    c_ass_meta = ((c.get("assortment") or {}).get("meta") or {})
                    c_href = (c_ass_meta.get("href") or "").strip()
                    if not c_href or c_qty <= 0:
                        continue

                    # цена компонента: пробуем получить сущность по href и взять salePrices
                    comp = self.get_assortment_by_href(c_href)
                    comp_price = self._pick_sale_price_value(comp) if comp else 0

                    _remember(c_href, c_ass_meta, qty * c_qty, comp_price)
            else:
                price = self._pick_sale_price_value(a)
                _remember(a_href, a_meta, qty, price)

        move_positions: List[Dict[str, Any]] = []
        for href, q in agg.items():
            if q <= 0:
                continue
            meta = meta_by_href.get(href)
            if not meta:
                # fallback meta восстановим по href (type из URL)
                parts = href.rstrip("/").split("/")
                ent_type = parts[-2] if len(parts) >= 2 else "assortment"
                meta = {"href": href, "type": ent_type, "mediaType": "application/json"}
            move_positions.append(
                {
                    "assortment": {"meta": meta},
                    "quantity": q,
                    "price": int(price_by_href.get(href, 0)),
                }
            )

        return move_positions

    def upsert_move_linked_to_order(
        self,
        *,
        order_number: str,
        customerorder: Dict[str, Any],
        items: List[Dict[str, Any]],
        dry_run: bool,
    ) -> Dict[str, Any]:
        """
        Move создаем/обновляем всегда, если demand еще нет.
        Обязательная связь: customerOrder (для "Связанные документы").
        Позиции Move = позиции заказа, но:
          - комплекты (bundle) в Move запрещены -> раскрываем в компоненты.
        Пытаемся провести move. Если ошибка остатков (3007) -> оставляем непроведенным.
        """
        existing = self.find_move_by_external_code(order_number)

        payload: Dict[str, Any] = {
            "externalCode": order_number,
            "name": order_number,
            "organization": ms_meta("organization", self.cfg.organization_id),
            "sourceStore": ms_meta("store", self.cfg.store_src_id),
            "targetStore": ms_meta("store", self.cfg.store_fbo_id),
            "state": ms_meta("state", self.cfg.state_move_supply_id),
            "customerOrder": {"meta": (customerorder.get("meta") or {})},  # связь Move ↔ Order
            "description": (customerorder.get("description") or ""),       # комментарий как у заказа
            "applicable": False,  # сначала позиции, потом пробуем провести
        }

        if dry_run:
            return existing or {"id": "DRYRUN", "meta": {"href": "DRYRUN"}, "applicable": False}

        if existing:
            mv = self.update_move(existing["id"], payload)
        else:
            mv = self.create_move(payload)

        move_positions = self._expand_items_to_move_positions(items)
        self.replace_move_positions(mv["id"], move_positions)

        # ----- try conduct move -----
        try:
            mv2 = self.update_move(mv["id"], {"applicable": True})
            return mv2
        except HttpError as e:
            # 3007: "Нельзя переместить товар, которого нет на складе"
            if e.status in (400, 412) and (
                "3007" in (e.text or "") or "Нельзя переместить" in (e.text or "") or "нет на складе" in (e.text or "")
            ):
                self.update_move(mv["id"], {"applicable": False})
                mv["applicable"] = False
                return mv
            raise

    # ---------- demand ----------

    def find_demand_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base}/entity/demand"
        params = {"filter": f"externalCode={external_code}"}
        rep = request_json("GET", url, headers=self._headers(), params=params, timeout=60)
        rows = rep.get("rows") or []
        return rows[0] if rows else None

    def create_demand_from_customerorder(self, *, customerorder: Dict[str, Any], order_number: str) -> Dict[str, Any]:
        """
        Demand создаем НЕ пустой:
        - позиции берем из CustomerOrder
        - salesChannel + description дублируем
        - номер demand оставляем как в МС (хронология), поэтому name не задаем
        """
        url = f"{self.base}/entity/demand"

        co_id = customerorder["id"]
        co_positions = self.get_customerorder_positions(co_id)

        positions: List[Dict[str, Any]] = []
        for p in co_positions:
            ass_meta = ((p.get("assortment") or {}).get("meta") or None)
            qty = p.get("quantity")
            price = p.get("price") or 0
            if not ass_meta or qty is None:
                continue
            positions.append({"assortment": {"meta": ass_meta}, "quantity": qty, "price": price})

        payload: Dict[str, Any] = {
            "externalCode": order_number,
            "organization": ms_meta("organization", self.cfg.organization_id),
            "agent": ms_meta("counterparty", self.cfg.counterparty_ozon_id),
            "store": ms_meta("store", self.cfg.store_fbo_id),
            "state": ms_meta("state", self.cfg.state_demand_fbo_id),
            "customerOrder": {"meta": (customerorder.get("meta") or {})},
            "salesChannel": ms_meta("saleschannel", self.cfg.sales_channel_id),
            "description": (customerorder.get("description") or ""),
            "positions": positions,
        }

        return request_json("POST", url, headers=self._headers(), json_body=payload, timeout=90)
