from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from app.http import HttpError
from app.moysklad_supply_service import MoySkladSupplyService, ms_moment_from_date
from app.ozon_supply_client import OzonSupplyClient


STATES_ALL = [
    "AWAITING_DELIVERY",
    "READY_TO_SUPPLY",
    "REPORTS_CONFIRMATION_AWAITING",
    "IN_TRANSIT",
    "ACCEPTANCE_AT_STORAGE_WAREHOUSE",
    "COMPLETED",
    "CANCELLED",
]

DEMAND_STATES = {"IN_TRANSIT", "ACCEPTANCE_AT_STORAGE_WAREHOUSE"}


@dataclass(frozen=True)
class CabinetRuntime:
    name: str
    ozon: OzonSupplyClient
    ms: MoySkladSupplyService


def _parse_timeslot_date(core: Dict[str, Any]) -> str:
    ts = ((core.get("timeslot") or {}).get("timeslot") or {})
    v = str(ts.get("from") or "").strip()
    if not v:
        return ""
    return v.split("T", 1)[0]


def _planned_moment(core: Dict[str, Any]) -> str:
    d = _parse_timeslot_date(core)
    if not d:
        return ""
    return ms_moment_from_date(d, "00", "00", "00")


def _planned_from_env() -> Optional[str]:
    return (os.environ.get("FBO_PLANNED_FROM") or "").strip() or None


def _planned_after(core: Dict[str, Any], planned_from: Optional[str]) -> bool:
    if not planned_from:
        return True
    d = _parse_timeslot_date(core)
    return bool(d and d >= planned_from)


def _normalize_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for it in items or []:
        offer_id = it.get("offer_id") or it.get("offerId")
        qty = it.get("quantity") or it.get("qty")
        if not offer_id or not qty:
            continue
        try:
            q = float(qty)
        except Exception:
            continue
        if q > 0:
            out.append({"offer_id": str(offer_id), "quantity": q})
    return out


def sync_fbo_supplies(*, ms_token: str, cabinets: List[CabinetRuntime]) -> None:
    dry_run = os.environ.get("FBO_DRY_RUN", "1") == "1"
    allow_delete = os.environ.get("FBO_ALLOW_DELETE", "0") == "1"
    planned_from = _planned_from_env()

    for rt in cabinets:
        orders = rt.ozon.iter_supply_orders_full(states=STATES_ALL)

        for core in orders:
            order_number = str(core.get("order_number") or "").strip()
            state = str(core.get("state") or "").strip()
            if not order_number:
                continue

            if not _planned_after(core, planned_from):
                continue

            shipment_planned = _planned_moment(core)

            demand = rt.ms.find_demand_by_external_code(order_number)
            if demand:
                if shipment_planned and not dry_run:
                    co = rt.ms.find_customerorder_by_external_code(order_number)
                    if co:
                        rt.ms.update_customerorder(co["id"], {"shipmentPlannedMoment": shipment_planned})
                print(f"[{rt.name}] {order_number} skip: demand exists")
                continue

            if allow_delete and state == "CANCELLED":
                if dry_run:
                    print(f"[{rt.name}] {order_number} SAFE: would delete")
                else:
                    mv = rt.ms.find_move_by_external_code(order_number)
                    co = rt.ms.find_customerorder_by_external_code(order_number)
                    if mv:
                        rt.ms.delete_move(mv["id"])
                    if co:
                        rt.ms.delete_customerorder(co["id"])
                    print(f"[{rt.name}] {order_number} deleted")
                continue

            supplies = core.get("supplies") or []
            bundle_id = str(supplies[0].get("bundle_id") or "").strip() if supplies else ""
            if not bundle_id:
                print(f"[{rt.name}] {order_number} WARN: no bundle_id")
                continue

            raw_items = rt.ozon.iter_bundle_items(bundle_id=bundle_id)
            items = _normalize_items(raw_items)
            if not items:
                print(f"[{rt.name}] {order_number} WARN: empty items")
                continue

            co = rt.ms.upsert_customerorder(
                order_number=order_number,
                shipment_planned_moment=shipment_planned,
                core=core,
                items=items,
                dry_run=dry_run,
            )

            mv = rt.ms.upsert_move_for_customerorder(
                order_number=order_number,
                customerorder=co,
                dry_run=dry_run,
            )

            if not mv.get("applicable"):
                print(f"[{rt.name}] {order_number} move not conducted")
                continue

            if state in DEMAND_STATES:
                if dry_run:
                    print(f"[{rt.name}] {order_number} DRYRUN: demand")
                else:
                    try:
                        rt.ms.create_demand_from_customerorder(co, order_number)
                        print(f"[{rt.name}] {order_number} demand created")
                    except HttpError as e:
                        if e.status_code == 412 and "3007" in str(e):
                            print(f"[{rt.name}] {order_number} no stock for demand")
                        else:
                            raise
            else:
                print(f"[{rt.name}] {order_number} done: state={state}")
