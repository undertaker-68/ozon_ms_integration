from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from app.ozon_supply_client import OzonCabinet, OzonSupplyClient
from app.moysklad_supply_service import MoySkladSupplyService, MsFboConfig

STATES_ALL = [
    "DATA_FILLING",
    "READY_TO_SUPPLY",
    "ACCEPTED_AT_SUPPLY_WAREHOUSE",
    "IN_TRANSIT",
    "ACCEPTANCE_AT_STORAGE_WAREHOUSE",
    "REPORTS_CONFIRMATION_AWAITING",
    "REPORT_REJECTED",
    "COMPLETED",
    "REJECTED_AT_SUPPLY_WAREHOUSE",
    "CANCELLED",
    "OVERDUE",
]

STATE_CREATE_DEMAND = {"IN_TRANSIT", "ACCEPTANCE_AT_STORAGE_WAREHOUSE"}
STATE_CANCELLED = {"CANCELLED"}  # удаление только по CANCELLED (как было)


@dataclass(frozen=True)
class CabinetRuntime:
    cabinet: OzonCabinet
    ms_cfg: MsFboConfig  # для salesChannel (кабинетный)


def _env_bool(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "y", "on")


def _parse_iso_dt(s: str) -> datetime:
    s = (s or "").strip()
    if not s:
        raise ValueError("empty iso datetime")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def _parse_offset_seconds(s: str) -> int:
    s = (s or "").strip().lower()
    if s.endswith("s"):
        s = s[:-1]
    return int(s or "0")


def _planned_local_date(order: Dict[str, Any]) -> Optional[date]:
    """
    Берём дату плановой отгрузки по Ozon:
      order.timeslot.timeslot.from + order.timeslot.timezone_info.offset
    Возвращаем локальную DATE.
    """
    ts = (order.get("timeslot") or {}).get("timeslot") or {}
    from_iso = ts.get("from")
    if not from_iso:
        return None

    tz_info = (order.get("timeslot") or {}).get("timezone_info") or {}
    offset_s = _parse_offset_seconds(tz_info.get("offset") or "0s")
    tz = timezone(timedelta(seconds=offset_s))

    dt_utc = _parse_iso_dt(from_iso).astimezone(timezone.utc)
    dt_local = dt_utc.astimezone(tz)
    return dt_local.date()


def _planned_ms_moment(d: date) -> str:
    # Формат, который МС стабильно принимает/показывает:
    # "YYYY-MM-DD HH:MM:SS.mmm" (без таймзоны)
    return f"{d.isoformat()} 00:00:00.000"

def sync_fbo_supplies(*, ms_token: str, cabinets: List[CabinetRuntime]) -> None:
    dry_run = _env_bool("FBO_DRY_RUN", "0")
    allow_delete = _env_bool("FBO_ALLOW_DELETE", "0")

    planned_from_str = os.environ.get("FBO_PLANNED_FROM", "2025-12-03").strip()  # YYYY-MM-DD
    planned_from = datetime.fromisoformat(planned_from_str).date()

    for c in cabinets:
        oz = OzonSupplyClient(c.cabinet)
        ms = MoySkladSupplyService(ms_token=ms_token, cfg=c.ms_cfg)

        orders = oz.iter_supply_orders_full(states=STATES_ALL, limit=100, batch_get=50)

        for order in orders:
            order_number = str(order.get("order_number") or "").strip()
            if not order_number:
                continue

            pd = _planned_local_date(order)
            if not pd:
                continue
            if pd < planned_from:
                continue

            state = str(order.get("state") or "").strip()

            # 1) если demand уже есть -> пропуск полностью
            if ms.find_demand_by_external_code(order_number):
                print(f"[{c.cabinet.name}] {order_number} skip: demand exists")
                continue

            # 2) отмена -> удалить move + order (если demand нет)
            if state in STATE_CANCELLED:
                if dry_run or not allow_delete:
                    mode = "DRYRUN" if dry_run else "SAFE"
                    print(f"[{c.cabinet.name}] {order_number} {mode}: would delete move+customerorder (no demand)")
                    continue

                mv = ms.find_move_by_external_code(order_number)
                if mv:
                    ms.delete_move(mv["id"])
                co = ms.find_customerorder_by_external_code(order_number)
                if co:
                    ms.delete_customerorder(co["id"])
                print(f"[{c.cabinet.name}] {order_number} deleted (cancelled)")
                continue

            # 3) позиции поставки (offer_id + quantity)
            items = oz.get_supply_order_items(order)

            # 4) upsert CustomerOrder (всегда applicable=true) + planned date
            shipment_moment = _planned_ms_moment(pd)
            print(f"[{c.cabinet.name}] {order_number} upsert customerorder state={state}")
            co = ms.upsert_customerorder(
                order_number=order_number,
                shipment_planned_moment=shipment_moment,
                core=order,
                items=items,
                dry_run=dry_run,
            )
            if dry_run:
                continue

            # 5) upsert Move, связанный с заказом, позиции = как в заказе (bundle раскрываем)
            mv = ms.upsert_move_linked_to_order(order_number=order_number, customerorder=co, items=items, dry_run=False)

            mv_applicable = bool(mv.get("applicable"))

            # 6) demand создаем только при статусах IN_TRANSIT / ACCEPTANCE... и только если Move проведен
            if state in STATE_CREATE_DEMAND:
                if not mv_applicable:
                    print(f"[{c.cabinet.name}] {order_number} skip demand: move not applicable")
                    continue

                # идемпотентность
                if ms.find_demand_by_external_code(order_number):
                    print(f"[{c.cabinet.name}] {order_number} skip: demand exists")
                    continue

                ms.create_demand_from_customerorder(customerorder=co, order_number=order_number)
                print(f"[{c.cabinet.name}] {order_number} demand created")
            else:
                # статусы, где demand не нужен — просто обновили заказ и move
                print(f"[{c.cabinet.name}] {order_number} done: state={state}")
