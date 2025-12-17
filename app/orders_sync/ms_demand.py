from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.http import HttpError

from .constants import (
    MS_COUNTERPARTY_OZON_ID,
    MS_STORE_OZON_ID,
    MS_DEMAND_STATE_FBS_ID,
    MS_ORGANIZATION_ID,
)
from .ms_meta import ms_meta, ms_demand_state_meta, ms_sales_channel_meta
from .assortment import extract_sale_price_cents


class DemandService:
    def __init__(self, ms):
        self.ms = ms

    # --- helpers -------------------------------------------------

    def _list_demands_by_customerorder(self, customerorder: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Ищем Demand по связанному документу customerOrder (как ты и просил).
        В МС фильтрация по customerOrder иногда капризная — поэтому делаем try/fallback.
        """
        co_meta = customerorder.get("meta") or {}
        co_href = co_meta.get("href")
        if not co_href:
            return []

        # Попытка №1: filter по customerOrder
        try:
            resp = self.ms.get(
                "/entity/demand",
                params={"filter": f'customerOrder={co_href}', "limit": 100},
            )
            return resp.get("rows") or []
        except HttpError:
            pass

        # Попытка №2: иногда требуется в кавычках
        try:
            resp = self.ms.get(
                "/entity/demand",
                params={"filter": f'customerOrder="{co_href}"', "limit": 100},
            )
            return resp.get("rows") or []
        except HttpError:
            pass

        # Фолбэк: если фильтр не поддержался на аккаунте/версии, ничего не возвращаем
        return []

    def delete_demand(self, demand_id: str) -> None:
        self.ms.delete(f"/entity/demand/{demand_id}")

    def ensure_single_demand_for_order(self, customerorder: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Если по заказу уже есть 2-3 demand — оставляем одну, остальные удаляем.
        """
        demands = self._list_demands_by_customerorder(customerorder)
        if not demands:
            return None

        demands_sorted = sorted(demands, key=lambda d: d.get("moment") or "")
        keep = demands_sorted[0]
        extras = demands_sorted[1:]

        for d in extras:
            did = d.get("id")
            if did:
                self.delete_demand(did)

        return keep

    def _get_customerorder_positions(self, customerorder: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Берём позиции заказа покупателя. Цена должна браться из заказа.
        Если цена 0 — лечим по "Цена продажи" из ассортимента.
        """
        co_id = customerorder["id"]
        pos = self.ms.get(f"/entity/customerorder/{co_id}/positions", params={"limit": 1000})
        rows = pos.get("rows") or []

        out: List[Dict[str, Any]] = []
        for r in rows:
            ass_meta = ((r.get("assortment") or {}).get("meta") or {})
            if not ass_meta.get("href"):
                continue

            qty = float(r.get("quantity") or 0)
            price = int(r.get("price") or 0)

            if price <= 0:
                # лечим цену по "Цена продажи"
                ass = self.ms.get(ass_meta["href"])
                price = int(extract_sale_price_cents(ass) or 0)

            out.append(
                {
                    "assortment": {"meta": ass_meta},
                    "quantity": qty,
                    "price": price,
                }
            )
        return out

    def _demand_positions(self, demand_id: str) -> List[Dict[str, Any]]:
        pos = self.ms.get(f"/entity/demand/{demand_id}/positions", params={"limit": 1000})
        return pos.get("rows") or []

    def _fill_demand_positions_if_empty(self, demand: Dict[str, Any], customerorder: Dict[str, Any]) -> None:
        """
        Если demand пустая (0 позиций) — добавляем позиции из заказа.
        """
        demand_id = demand["id"]
        existing_rows = self._demand_positions(demand_id)
        if existing_rows:
            return

        rows = self._get_customerorder_positions(customerorder)
        if not rows:
            return

        # добавляем позиции в demand
        self.ms.post(f"/entity/demand/{demand_id}/positions", json={"rows": rows})

    def _fix_demand_prices_zero(self, demand: Dict[str, Any], customerorder: Dict[str, Any]) -> None:
        """
        Если в demand есть позиции, но price=0 — проставляем цену (как в заказе; если 0, то цена продажи).
        Обновляем позицию по meta.href.
        """
        demand_id = demand["id"]
        rows = self._demand_positions(demand_id)

        # Карту "assortment href -> price" берём из заказа
        co_rows = self._get_customerorder_positions(customerorder)
        price_by_assort_href = {}
        for r in co_rows:
            ah = (((r.get("assortment") or {}).get("meta") or {}).get("href")) or ""
            if ah:
                price_by_assort_href[ah] = int(r.get("price") or 0)

        for r in rows:
            if int(r.get("price") or 0) != 0:
                continue

            rmeta = (r.get("meta") or {})
            rhref = rmeta.get("href")
            if not rhref:
                continue

            ass_href = ((((r.get("assortment") or {}).get("meta") or {}).get("href")) or "")
            price = int(price_by_assort_href.get(ass_href) or 0)

            if price <= 0 and ass_href:
                ass = self.ms.get(ass_href)
                price = int(extract_sale_price_cents(ass) or 0)

            if price > 0:
                self.ms.put(rhref, json={"price": price})

    # --- main ----------------------------------------------------

    def create_from_customerorder_if_missing(
        self,
        *,
        customerorder: Dict[str, Any],
        posting_number: str,
        sales_channel_id: str,
        demand_state_id: str = MS_DEMAND_STATE_FBS_ID,
    ) -> Dict[str, Any]:
        """
        Создаём Demand (Отгрузку) из CustomerOrder.
        Красиво:
        - определяем "есть ли уже demand" по связанному customerOrder (не по номеру)
        - если demand уже есть: удаляем лишние, заполняем если пустая, лечим цены
        - если нет: создаём сразу с позициями и правильными ценами
        """
        # 1) Если уже есть demand(ы) по заказу — оставить одну, остальное удалить
        existing = self.ensure_single_demand_for_order(customerorder)
        if existing:
            self._fill_demand_positions_if_empty(existing, customerorder)
            self._fix_demand_prices_zero(existing, customerorder)
            return existing

        # 2) Создаём новую demand СРАЗУ с позициями
        co_meta = customerorder.get("meta")
        if not co_meta:
            raise ValueError("customerorder.meta is missing (cannot create demand)")

        rows = self._get_customerorder_positions(customerorder)

        payload: Dict[str, Any] = {
            "externalCode": posting_number,  # удобно для поиска/аналитики, но не полагаемся на него
            "agent": ms_meta("counterparty", MS_COUNTERPARTY_OZON_ID),
            "store": ms_meta("store", MS_STORE_OZON_ID),
            "organization": ms_meta("organization", MS_ORGANIZATION_ID),
            "customerOrder": {"meta": co_meta},
            "state": ms_demand_state_meta(demand_state_id),
            "salesChannel": ms_sales_channel_meta(sales_channel_id),
            "positions": {"rows": rows},
        }

        try:
            created = self.ms.post("/entity/demand", json=payload)
        except HttpError as e:
            http_status = getattr(e, "status_code", None) or getattr(e, "status", None) or getattr(e, "code", None)
            body = getattr(e, "message", None) or getattr(e, "text", None) or str(e)

            # Нельзя отгрузить товар, которого нет на складе → просто пропускаем
            if http_status == 412 and "3007" in body:
                return None
            raise

        if not created:
            return None

        # 3) На всякий случай (если МС что-то сконвертил) — лечим нули
        self._fill_demand_positions_if_empty(created, customerorder)
        self._fix_demand_prices_zero(created, customerorder)

        return created
