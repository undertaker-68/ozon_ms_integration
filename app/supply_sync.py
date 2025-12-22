from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.http import HttpError
from app.moysklad_supply_service import MoySkladSupplyService, MsFboConfig, ms_moment_from_date
from app.ozon_supply_client import OzonSupplyClient

# Статусы поставок/заказов, которые мы тянем
STATES_ALL = [
    "AWAITING_DELIVERY",
    "READY_TO_SUPPLY",
    "REPORTS_CONFIRMATION_AWAITING",
    "IN_TRANSIT",
    "ACCEPTANCE_AT_STORAGE_WAREHOUSE",
    "COMPLETED",
    "CANCELLED",
]

# Только эти статусы должны создавать Demand
DEMAND_STATES = {"IN_TRANSIT", "ACCEPTANCE_AT_STORAGE_WAREHOUSE"}


@dataclass(frozen=True)
class CabinetRuntime:
    name: str
    ozon: OzonSupplyClient
    ms: MoySkladSupplyService


def _parse_ozon_timeslot_date(core: Dict[str, Any]) -> str:
    """
    По ТЗ: всегда timeslot.timeslot.from
    Берём только дату YYYY-MM-DD
    """
    try:
        ts = ((core.get("timeslot") or {}).get("timeslot") or {})
        s = str(ts.get("from") or "").strip()
        # ожидаем ISO, например 2025-12-17T12:00:00Z
        if not s:
            return ""
        return s.split("T", 1)[0]
    except Exception:
        return ""


def _planned_moment_ms(core: Dict[str, Any]) -> str:
    d = _parse_ozon_timeslot_date(core)
    if not d:
        return ""
    # время любое, главное дата
    return ms_moment_from_date(d, "00", "00", "00")


def _planned_from_env() -> Optional[str]:
    """
    FBO_PLANNED_FROM=YYYY-MM-DD
    """
    s = str(os.environ.get("FBO_PLANNED_FROM") or "").strip()
    return s or None


def _is_planned_after_threshold(core: Dict[str, Any], planned_from: Optional[str]) -> bool:
    if not planned_from:
        return True
    d = _parse_ozon_timeslot_date(core)
    if not d:
        return False
    try:
        return d >= planned_from
    except Exception:
        return False


def _normalize_bundle_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Унификация структуры items из Ozon bundle endpoint:
    выход: [{"offer_id": "...", "quantity": N}, ...]
    """
    out: List[Dict[str, Any]] = []
    for it in items or []:
        offer_id = it.get("offer_id") or it.get("offerId") or it.get("offerID")
        qty = it.get("quantity") or it.get("qty") or it.get("count")
        # некоторые ответы могут отдавать вложенно
        if not offer_id and isinstance(it.get("product"), dict):
            offer_id = it["product"].get("offer_id") or it["product"].get("offerId")
        if qty is None and isinstance(it.get("product"), dict):
            qty = it["product"].get("quantity") or it["product"].get("qty")

        offer_id = str(offer_id or "").strip()
        try:
            q = float(qty or 0)
        except Exception:
            q = 0

        if offer_id and q > 0:
            out.append({"offer_id": offer_id, "quantity": q})
    return out


def sync_fbo_supplies(*, ms_token: str, cabinets: List[Dict[str, Any]]) -> None:
    dry_run = str(os.environ.get("FBO_DRY_RUN") or "1").strip() == "1"
    allow_delete = str(os.environ.get("FBO_ALLOW_DELETE") or "0").strip() == "1"
    planned_from = _planned_from_env()

    runtimes: List[CabinetRuntime] = []
    for cab in cabinets:
        name = cab["name"]
        oz: OzonSupplyClient = cab["ozon"]
        ms_cfg: MsFboConfig = cab["ms_cfg"]
        ms = MoySkladSupplyService(ms_token=ms_token, cfg=ms_cfg)
        runtimes.append(CabinetRuntime(name=name, ozon=oz, ms=ms))

    for rt in runtimes:
        orders = rt.ozon.iter_supply_orders_full(states=STATES_ALL, limit=100, batch_get=50)

        for core in orders:
            order_number = str(core.get("order_number") or "").strip()
            state = str(core.get("state") or "").strip()
            if not order_number:
                continue

            # фильтр по плановой дате отгрузки
            if not _is_planned_after_threshold(core, planned_from):
                continue

            shipment_planned_moment = _planned_moment_ms(core)

            # Проверяем demand существование (по externalCode=order_number)
            demand = rt.ms.find_demand_by_external_code(order_number)
            if demand:
                # По твоей логике сейчас: если Demand есть — позиции/Move не трогаем.
                # НО плановую дату можно обновить в CustomerOrder (чтобы везде была верная).
                co = rt.ms.find_customerorder_by_external_code(order_number)
                if co and not dry_run and shipment_planned_moment:
                    rt.ms.update_customerorder(co["id"], {"shipmentPlannedMoment": shipment_planned_moment})
                print(f"[{rt.name}] {order_number} skip: demand exists")
                continue

            # Если статусы "В пути" / "Приемка", но demand нет — НЕ удаляем, просто пропускаем (как ты сказал)
            if state in ("IN_TRANSIT", "ACCEPTANCE_AT_STORAGE_WAREHOUSE") and demand is None:
                # но order/move должны существовать; если нет — создадим ниже (это ок)
                pass

            # Удаление (только если разрешено и demand нет)
            if allow_delete and state == "CANCELLED":
                co = rt.ms.find_customerorder_by_external_code(order_number)
                mv = rt.ms.find_move_by_external_code(order_number)
                if dry_run:
                    print(f"[{rt.name}] {order_number} SAFE: would delete move+customerorder (no demand)")
                else:
                    if mv and mv.get("id"):
                        rt.ms.delete_move(mv["id"])
                    if co and co.get("id"):
                        rt.ms.delete_customerorder(co["id"])
                    print(f"[{rt.name}] {order_number} deleted (cancelled)")
                continue

            # --- Получаем items через bundle_id ---
            bundle_id = ""
            try:
                supplies = (core.get("supplies") or [])
                if supplies and isinstance(supplies, list):
                    bundle_id = str(supplies[0].get("bundle_id") or "").strip()
            except Exception:
                bundle_id = ""

            if not bundle_id:
                # По твоим словам пустых поставок не бывает; если тут пусто — логируем и пропускаем
                print(f"[{rt.name}] {order_number} WARN: no bundle_id in core -> skip")
                continue

            raw_items = rt.ozon.iter_bundle_items(bundle_id=bundle_id)
            items = _normalize_bundle_items(raw_items)

            if not items:
                # Это и было твоей проблемой: "часть заказов пустые"
                # Теперь мы явно покажем проблему по bundle_id/items.
                print(f"[{rt.name}] {order_number} WARN: bundle has no items (bundle_id={bundle_id}) -> skip")
                continue

            # --- Upsert CustomerOrder (always conducted) ---
            co = rt.ms.upsert_customerorder(
                order_number=order_number,
                shipment_planned_moment=shipment_planned_moment,
                core=core,
                items=items,
                dry_run=dry_run,
            )
            print(f"[{rt.name}] {order_number} upsert customerorder state={state}")

            # --- Upsert Move linked to CustomerOrder ---
            mv = rt.ms.upsert_move_for_customerorder(order_number=order_number, customerorder=co, dry_run=dry_run)

            applicable = bool(mv.get("applicable"))
            if not applicable:
                # если move не проведён — demand НЕ создаём
                print(f"[{rt.name}] {order_number} done: move not conducted -> demand not created")
                continue

            # --- Create demand only for specific states ---
            if state in DEMAND_STATES:
                if dry_run:
                    print(f"[{rt.name}] {order_number} DRYRUN: would create demand")
                    continue
                try:
                    rt.ms.create_demand_from_customerorder(customerorder=co, order_number=order_number)
                    print(f"[{rt.name}] {order_number} demand created")
                except HttpError as e:
                    body = getattr(e, "body", "") or str(e)
                    # 3007: нет товара — просто НЕ создаём demand и не падаем
                    if e.status_code == 412 and "3007" in body:
                        print(f"[{rt.name}] {order_number} demand NOT created (not enough stock)")
                    else:
                        raise
            else:
                print(f"[{rt.name}] {order_number} done: state={state}")
