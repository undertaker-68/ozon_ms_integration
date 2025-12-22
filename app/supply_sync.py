from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from app.ozon_supply_client import OzonCabinet, OzonSupplyClient
from app.moysklad_supply_service import MoySkladSupplyService, MsFboConfig
from app.http import HttpError

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
STATE_CANCELLED = {"CANCELLED"}


@dataclass(frozen=True)
class CabinetRuntime:
    cabinet: OzonCabinet
    ms_cfg: MsFboConfig


def _env_bool(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "y", "on")


def _parse_iso_dt(s: str) -> datetime:
    s = (s or "").strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def _parse_offset_seconds(s: str) -> int:
    s = (s or "").strip().lower()
    if s.endswith("s"):
        s = s[:-1]
    return int(s or "0")


def _planned_local_date(order: Dict[str, Any]) -> Optional[date]:
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


def _planned_ms_moment_isoz(d: date) -> str:
    # Самый надежный формат для МС
    return f"{d.isoformat()}T00:00:00Z"


def sync_fbo_supplies(*, ms_token: str, cabinets: List[CabinetRuntime]) -> None:
    dry_run = _env_bool("FBO_DRY_RUN", "0")
    allow_delete = _env_bool("FBO_ALLOW_DELETE", "0")

    planned_from_str = os.environ.get("FBO_PLANNED_FROM", "2025-12-03").strip()
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
            if not pd or pd < planned_from:
                continue

            state = str(order.get("state") or "").strip()

            # если demand уже есть -> пропуск полностью
            if ms.find_demand_by_external_code(order_number):
                print(f"[{c.cabinet.name}] {order_number} skip: demand exists")
                continue

            # отмена
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

            # товары поставки (bundle -> items)
            items = oz.get_supply_order_items(order)

            # если вдруг items пустые — логируем (это и есть причина "пустых заказов")
            if not items:
                print(f"[{c.cabinet.name}] {order_number} WARN: empty items from bundle -> skip")
                continue

            shipment_moment = _planned_ms_moment_isoz(pd)

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

            mv = ms.upsert_move_linked_to_order(
                order_number=order_number, customerorder=co, items=items, dry_run=False
            )

            if state in STATE_CREATE_DEMAND:
                if not mv.get("applicable"):
                    print(f"[{c.cabinet.name}] {order_number} skip demand: move not applicable")
                    continue

                try:
                    ms.create_demand_from_customerorder(customerorder=co, order_number=order_number)
                    print(f"[{c.cabinet.name}] {order_number} demand created")
                except HttpError as e:
                    txt = e.text or ""
                    if e.status == 412 and ("3007" in txt or "Нельзя отгрузить" in txt or "нет на складе" in txt):
                        print(f"[{c.cabinet.name}] {order_number} skip demand: no stock (3007)")
                        continue
                    raise
            else:
                print(f"[{c.cabinet.name}] {order_number} done: state={state}")
