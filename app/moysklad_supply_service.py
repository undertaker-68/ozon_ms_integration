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


def ms_moment_from_date(date_yyyy_mm_dd: str, hh: str = "00", mm: str = "00", ss: str = "00") -> str:
    """
    МойСклад ожидает moment как строку вида: YYYY-MM-DD HH:MM:SS.000
    """
    if not date_yyyy_mm_dd:
        return ""
    return f"{date_yyyy_mm_dd} {hh}:{mm}:{ss}.000"


@dataclass(frozen=True)
class MsFboConfig:
    organization_id: str
    counterparty_ozon_id: str

    store_src_id: str  # склад-источник (откуда перемещаем)
    store_fbo_id: str  # склад-назначение (FBO)

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

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.ms_token}",
            "Content-Type": "application/json",
            "Accept": "application/json;charset=utf-8",
        }

    # ---------------- Bundles (MS) ----------------

    def get_bundle(self, bundle_id: str) -> Dict[str, Any]:
        url = f"{self.base}/entity/bundle/{bundle_id}"
        return request_json("GET", url, headers=self._headers(), timeout=90)

    # ---------------- Assortment + Price ----------------

    def find_assortment_by_article(self, article: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base}/entity/assortment"
        params = {"filter": f"article={article}"}
        rep = request_json("GET", url, headers=self._headers(), params=params, timeout=90)
        rows = rep.get("rows") or []
        return rows[0] if rows else None

    def _pick_sale_price_value(self, assortment: Dict[str, Any]) -> int:
        sale_prices = assortment.get("salePrices") or []
        if not sale_prices:
            return 0
        # Берём первую цену (как было у тебя ранее)
        return int(sale_prices[0].get("value") or 0)

    # ---------------- CustomerOrder ----------------

    def find_customerorder_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base}/entity/customerorder"
        params = {"filter": f"externalCode={external_code}"}
        rep = request_json("GET", url, headers=self._headers(), params=params, timeout=90)
        rows = rep.get("rows") or []
        return rows[0] if rows else None

    def create_customerorder(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base}/entity/customerorder"
        return request_json("POST", url, headers=self._headers(), json_body=payload, timeout=90)

    def update_customerorder(self, customerorder_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base}/entity/customerorder/{customerorder_id}"
        return request_json("PUT", url, headers=self._headers(), json_body=payload, timeout=90)

    def replace_customerorder_positions(self, customerorder_id: str, positions: List[Dict[str, Any]]) -> None:
        """
        У вас корректно работает POST массива позиций.
        Поэтому делаем: GET -> DELETE -> POST.
        """
        list_url = f"{self.base}/entity/customerorder/{customerorder_id}/positions"

        rep = request_json("GET", list_url, headers=self._headers(), params={"limit": 1000}, timeout=180)
        rows = rep.get("rows") or []
        for r in rows:
            pid = r.get("id")
            if pid:
                request_json("DELETE", f"{list_url}/{pid}", headers=self._headers(), timeout=90)

        if positions:
            request_json("POST", list_url, headers=self._headers(), json_body=positions, timeout=180)

    def get_customerorder_positions(self, customerorder_id: str) -> List[Dict[str, Any]]:
        url = f"{self.base}/entity/customerorder/{customerorder_id}/positions"
        rep = request_json("GET", url, headers=self._headers(), params={"limit": 1000}, timeout=180)
        return rep.get("rows") or []

    def delete_customerorder(self, customerorder_id: str) -> None:
        url = f"{self.base}/entity/customerorder/{customerorder_id}"
        request_json("DELETE", url, headers=self._headers(), timeout=90)

    def _build_comment_once(self, order_number: str, core: Dict[str, Any]) -> str:
        """
        Комментарий пишем ТОЛЬКО при создании.
        Берём склад назначения (Уфа и т.д.), а не склад отправитель.
        """
        # Ожидаемые поля core — это кусок ozon order (из supply_order_get)
        # Пробуем storage_warehouse.name из supplies[0], если есть
        dest_name = ""
        try:
            supplies = (core.get("supplies") or [])
            if supplies and isinstance(supplies, list):
                sw = (supplies[0].get("storage_warehouse") or {})
                dest_name = str(sw.get("name") or "").strip()
        except Exception:
            dest_name = ""

        # fallback: drop_off_warehouse.name (если storage_warehouse почему-то пустой)
        if not dest_name:
            try:
                dow = (core.get("drop_off_warehouse") or {})
                dest_name = str(dow.get("name") or "").strip()
            except Exception:
                dest_name = ""

        if dest_name:
            return f"{order_number} → {dest_name}"
        return f"{order_number}"

    # ---------- items expansion (bundle -> components) ----------

    def _expand_items_to_component_positions(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Разворачиваем bundle в компоненты ДАЖЕ для CustomerOrder (по твоему требованию),
        чтобы Move потом собирался корректно.

        Агрегируем по компонентам (href), для каждой позиции ставим:
        assortment.meta + quantity + price (sale price)
        """
        agg_qty: Dict[str, float] = {}  # href -> qty
        meta_by_href: Dict[str, Dict[str, Any]] = {}  # href -> meta
        price_by_href: Dict[str, int] = {}  # href -> price

        def remember(href: str, meta: Dict[str, Any], qty: float, price: int) -> None:
            if not href or qty <= 0:
                return
            agg_qty[href] = agg_qty.get(href, 0.0) + qty
            if href not in meta_by_href and meta:
                meta_by_href[href] = meta
            if href not in price_by_href:
                price_by_href[href] = int(price or 0)

        for it in items or []:
            offer_id = str(it.get("offer_id") or it.get("offerId") or "").strip()
            qty = float(it.get("quantity") or it.get("qty") or 0)
            if not offer_id or qty <= 0:
                continue

            a = self.find_assortment_by_article(offer_id)
            if not a:
                continue

            a_meta = (a.get("meta") or {})
            a_type = str(a_meta.get("type") or "").lower()
            a_href = str(a_meta.get("href") or "").strip()
            a_price = self._pick_sale_price_value(a)

            if a_type == "bundle" and a_href:
                bundle_id = a_href.rstrip("/").split("/")[-1]
                b = self.get_bundle(bundle_id)
                comps = ((b or {}).get("components") or {}).get("rows") or []
                for c in comps:
                    c_qty = float(c.get("quantity") or 0)
                    c_meta = ((c.get("assortment") or {}).get("meta") or {})
                    c_href = str(c_meta.get("href") or "").strip()
                    if not c_href or c_qty <= 0:
                        continue
                    # цену компонента берём как salePrice компонента
                    comp_id = c_href.rstrip("/").split("/")[-1]
                    comp_type = c_href.rstrip("/").split("/")[-2]
                    # найдём компонент как assortment через /entity/assortment?filter=... нельзя по id
                    # поэтому цену берём 0 если не можем
                    comp_price = 0
                    # если в meta есть type/ href — можно сделать GET entity/{type}/{id}
                    try:
                        comp = request_json(
                            "GET",
                            f"{self.base}/entity/{comp_type}/{comp_id}",
                            headers=self._headers(),
                            timeout=90,
                        )
                        comp_price = self._pick_sale_price_value(comp)
                        c_meta_full = (comp.get("meta") or c_meta)
                        remember(c_href, c_meta_full, qty * c_qty, comp_price)
                    except Exception:
                        # fallback: хотя бы meta из компонента бандла
                        remember(c_href, c_meta, qty * c_qty, comp_price)
            else:
                remember(a_href, a_meta, qty, a_price)

        positions: List[Dict[str, Any]] = []
        for href, q in agg_qty.items():
            meta = meta_by_href.get(href) or {}
            price = int(price_by_href.get(href) or 0)
            if not meta:
                # восстановим meta по href
                parts = href.rstrip("/").split("/")
                ent_type = parts[-2]
                meta = {"href": href, "type": ent_type, "mediaType": "application/json"}
            positions.append({"assortment": {"meta": meta}, "quantity": q, "price": price})
        return positions

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

        if shipment_planned_moment:
            payload["shipmentPlannedMoment"] = shipment_planned_moment

        if dry_run:
            return existing or {"id": "DRYRUN", "meta": {"href": "DRYRUN"}}

        if existing:
            # description НЕ обновляем (по ТЗ)
            co = self.update_customerorder(existing["id"], payload)
        else:
            payload["description"] = self._build_comment_once(order_number, core)
            co = self.create_customerorder(payload)

        # Позиции пересобираем полностью (снаружи гарантируем "если demand нет")
        positions = self._expand_items_to_component_positions(items)
        self.replace_customerorder_positions(co["id"], positions)

        return co

    # ---------------- Move ----------------

    def find_move_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base}/entity/move"
        params = {"filter": f"externalCode={external_code}"}
        rep = request_json("GET", url, headers=self._headers(), params=params, timeout=90)
        rows = rep.get("rows") or []
        return rows[0] if rows else None

    def create_move(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base}/entity/move"
        return request_json("POST", url, headers=self._headers(), json_body=payload, timeout=90)

    def update_move(self, move_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base}/entity/move/{move_id}"
        return request_json("PUT", url, headers=self._headers(), json_body=payload, timeout=90)

    def replace_move_positions(self, move_id: str, positions: List[Dict[str, Any]]) -> None:
        list_url = f"{self.base}/entity/move/{move_id}/positions"

        rep = request_json("GET", list_url, headers=self._headers(), params={"limit": 1000}, timeout=180)
        rows = rep.get("rows") or []

        for r in rows:
            pid = r.get("id")
            if not pid:
                continue
            request_json("DELETE", f"{list_url}/{pid}", headers=self._headers(), timeout=90)

        if positions:
            request_json("POST", list_url, headers=self._headers(), json_body=positions, timeout=180)

    def delete_move(self, move_id: str) -> None:
        url = f"{self.base}/entity/move/{move_id}"
        request_json("DELETE", url, headers=self._headers(), timeout=90)

    def _build_move_positions_from_customerorder_positions(self, co_positions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        В move bundle нельзя. Но мы уже разворачиваем bundle в компоненты прямо в CustomerOrder,
        поэтому здесь просто копируем позиции CO → Move (quantity+price).
        """
        out: List[Dict[str, Any]] = []
        for p in co_positions or []:
            ass = (p.get("assortment") or {}).get("meta") or {}
            qty = float(p.get("quantity") or 0)
            if not ass or qty <= 0:
                continue
            price = int(p.get("price") or 0)
            out.append({"assortment": {"meta": ass}, "quantity": qty, "price": price})
        return out

    def upsert_move_for_customerorder(
        self,
        *,
        order_number: str,
        customerorder: Dict[str, Any],
        dry_run: bool,
    ) -> Dict[str, Any]:
        """
        Move создаём/обновляем когда есть CustomerOrder.
        Move должен быть связан с Order (customerOrder.meta).
        Проводим Move, если можно; если 3007 — оставляем непроведенным.
        """
        existing = self.find_move_by_external_code(order_number)

        payload: Dict[str, Any] = {
            "externalCode": order_number if self.cfg.set_move_external_code else (existing or {}).get("externalCode", ""),
            "name": order_number,
            "organization": ms_meta("organization", self.cfg.organization_id),
            "sourceStore": ms_meta("store", self.cfg.store_src_id),
            "targetStore": ms_meta("store", self.cfg.store_fbo_id),
            "state": ms_meta("state", self.cfg.state_move_supply_id),
            "customerOrder": {"meta": (customerorder.get("meta") or {})},  # связь Move ↔ Order
            # комментарий: если в заказе есть description — используем его, но не перетираем если уже было
            "description": (customerorder.get("description") or ""),
            "applicable": False,  # сначала неприменимо, потом попробуем провести
        }

        if dry_run:
            return existing or {"id": "DRYRUN", "meta": {"href": "DRYRUN"}, "applicable": False}

        mv = self.update_move(existing["id"], payload) if existing else self.create_move(payload)

        # позиции move = позиции заказа (они уже без bundle)
        co_id = customerorder.get("id")
        co_positions = self.get_customerorder_positions(co_id) if co_id else []
        move_positions = self._build_move_positions_from_customerorder_positions(co_positions)
        self.replace_move_positions(mv["id"], move_positions)

        # попытка провести
        try:
            mv = self.update_move(mv["id"], {"applicable": True})
        except HttpError as e:
            body = getattr(e, "body", "") or str(e)
            if e.status_code == 412 and "3007" in body:
                # нет товара на складе → оставляем непроведенным
                mv = self.update_move(mv["id"], {"applicable": False})
            else:
                raise

        return mv

    # ---------------- Demand ----------------

    def find_demand_by_external_code(self, external_code: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base}/entity/demand"
        params = {"filter": f"externalCode={external_code}"}
        rep = request_json("GET", url, headers=self._headers(), params=params, timeout=90)
        rows = rep.get("rows") or []
        return rows[0] if rows else None

    def create_demand_from_customerorder(self, *, customerorder: Dict[str, Any], order_number: str) -> Dict[str, Any]:
        """
        Создаём отгрузку НЕ пустую:
        - customerOrder meta
        - salesChannel
        - store (FBO)
        - state (FBO demand state)
        - positions копируем из CustomerOrder.positions
        """
        url = f"{self.base}/entity/demand"

        co_id = customerorder.get("id")
        co_positions = self.get_customerorder_positions(co_id) if co_id else []
        rows: List[Dict[str, Any]] = []
        for p in co_positions:
            ass = (p.get("assortment") or {}).get("meta") or {}
            qty = float(p.get("quantity") or 0)
            if not ass or qty <= 0:
                continue
            price = int(p.get("price") or 0)
            rows.append({"assortment": {"meta": ass}, "quantity": qty, "price": price})

        payload: Dict[str, Any] = {
            # Важно: externalCode используем для поиска (как у тебя было)
            "externalCode": order_number,
            "organization": ms_meta("organization", self.cfg.organization_id),
            "agent": ms_meta("counterparty", self.cfg.counterparty_ozon_id),
            "store": ms_meta("store", self.cfg.store_fbo_id),
            "state": ms_meta("state", self.cfg.state_demand_fbo_id),
            "salesChannel": ms_meta("saleschannel", self.cfg.sales_channel_id),
            "customerOrder": {"meta": (customerorder.get("meta") or {})},
            "positions": {"rows": rows},
            # description не обязателен, но пусть будет как в заказе
            "description": (customerorder.get("description") or ""),
        }

        return request_json("POST", url, headers=self._headers(), json_body=payload, timeout=180)
