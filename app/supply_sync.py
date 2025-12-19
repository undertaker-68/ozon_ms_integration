from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List

from app.ozon_supply_client import OzonCabinet, OzonSupplyClient
from app.moysklad_supply_service import MoySkladSupplyService, MsFboConfig

# Статусы Ozon supply-order (из твоего списка)
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


def _parse_iso(dt: str) -> datetime:
    # Ozon приходит с Z
    dt = (dt or "").strip()
    if dt.endswith("Z"):
        dt = dt[:-1] + "+00:00"
    return datetime.fromisoformat(dt).astimezone(timezone.utc)


def _extract(order: Dict[str, Any]) -> Dict[str, Any]:
    supply_number = str(order.get("order_number") or "").strip()
    state = str(order.get("state") or "").strip()
    created_date = str(order.get("created_date") or "").strip()

    timeslot = ((order.get("timeslot") or {}).get("timeslot") or {})
    planned_from = str(timeslot.get("from") or "").strip()

    supplies = order.get("supplies") or []
    s0 = supplies[0] if supplies else {}
    bundle_id = str(s0.get("bundle_id") or "").strip()

    storage_wh = s0.get("storage_warehouse") or {}
    storage_name = str(storage_wh.get("name") or "").strip()

    description = f"{supply_number} - {storage_name}".strip(" -")

    return {
        "supply_number": supply_number,
        "state": state,
        "created_date": created_date,
        "planned_from": planned_from,
        "bundle_id": bundle_id,
        "description": description,
    }


def sync_fbo_supplies(
    *,
    ms_token: str,
    cabinets: List[CabinetRuntime],
    created_from_iso: str,  # теперь это порог по planned_from (timeslot.from), чтобы не ломать scripts/*
    dry_run: bool = True,
) -> None:
    planned_from_dt = _parse_iso(created_from_iso)
    allow_delete = os.environ.get("FBO_ALLOW_DELETE", "0").strip().lower() in ("1", "true", "yes")

    for c in cabinets:
        oz = OzonSupplyClient(c.cabinet)
        ms = MoySkladSupplyService(ms_token=ms_token, cfg=c.ms_cfg)

        orders = oz.iter_supply_orders_full(states=STATES_ALL, limit=100, batch_get=50)

        for order in orders:
            core = _extract(order)
            sn = core["supply_number"]
            if not sn:
                continue

            # ФИЛЬТР ПО ПЛАНОВОЙ ДАТЕ ОТГРУЗКИ (timeslot.from), а не по created_date
            pf = core["planned_from"]
            if not pf:
                continue
            if _parse_iso(pf) < planned_from_dt:
                continue

            # если в МС уже есть заказ и у него есть demand — пропуск полностью
            existing_co = ms.find_customerorder_by_external_code(sn)
            if existing_co:
                href = (existing_co.get("meta") or {}).get("href")
                if href:
                    demands = ms.list_demands_by_customerorder(href)
                    if demands:
                        print(f"[{c.cabinet.name}] {sn} skip: demand exists")
                        continue

            state = core["state"]

            # отмена
            if state in STATE_CANCELLED:
                if dry_run or not allow_delete:
                    mode = "DRYRUN" if dry_run else "SAFE"
                    print(f"[{c.cabinet.name}] {sn} {mode}: would delete move+customerorder (no demand)")
                    continue

                move = ms.find_move_by_external_code(sn)
                if move:
                    ms.delete_move(move["id"])
                co = ms.find_customerorder_by_external_code(sn)
                if co:
                    ms.delete_customerorder(co["id"])

                print(f"[{c.cabinet.name}] {sn} deleted (cancelled)")
                continue

            # обычный апсерт заказа+перемещения
            items: List[Dict[str, Any]] = []
            if core["bundle_id"]:
                items = oz.iter_bundle_items(bundle_id=core["bundle_id"])

            ms.upsert_supply_customerorder_and_move(
                supply_number=sn,
                shipment_planned_iso=core["planned_from"],
                description=core["description"],
                items=items,
                dry_run=dry_run,
            )
            print(f"[{c.cabinet.name}] {sn} upsert order+move state={state}")

            # create demand
            if state in STATE_CREATE_DEMAND:
                if dry_run:
                    print(f"[{c.cabinet.name}] {sn} DRYRUN: would create demand")
                    continue

                # пере-найдём заказ (после upsert он точно есть)
                co = ms.find_customerorder_by_external_code(sn)
                if not co:
                    print(f"[{c.cabinet.name}] {sn} WARN: customerorder missing after upsert")
                    continue

                ms.create_demand_from_customerorder(
                    {"meta": co["meta"]},
                    external_code=sn,
                    description=core["description"],
                )
                print(f"[{c.cabinet.name}] {sn} demand created")
