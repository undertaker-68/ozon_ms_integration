from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional

from app.ozon_client import OzonClient
from app.moysklad_supply_service import MoySkladSupplyService, MsFboConfig


# Ozon states
STATE_CANCELLED = {"CANCELLED", "REJECTED_AT_SUPPLY_WAREHOUSE", "OVERDUE"}
STATE_NEED_DEMAND = {"IN_TRANSIT", "ACCEPTANCE_AT_STORAGE_WAREHOUSE"}  # по ТЗ


def _parse_iso_dt(s: str) -> datetime:
    s = (s or "").strip()
    if not s:
        raise ValueError("empty iso datetime")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def _parse_offset_seconds(s: str) -> int:
    # "25200s" -> 25200
    s = (s or "").strip().lower()
    if s.endswith("s"):
        s = s[:-1]
    return int(s or "0")


def _planned_local_date(core: Dict[str, Any]) -> Optional[date]:
    """
    Берём дату плановой отгрузки по Ozon timeslot:
      timeslot.timeslot.from + timeslot.timezone_info.offset
    Возвращаем только DATE (локальную, по offset).
    """
    ts = (core.get("timeslot") or {}).get("timeslot") or {}
    from_iso = ts.get("from")
    if not from_iso:
        return None

    tz_info = (core.get("timeslot") or {}).get("timezone_info") or {}
    offset_s = _parse_offset_seconds(tz_info.get("offset") or "0s")
    tz = timezone(timedelta(seconds=offset_s))

    dt_utc = _parse_iso_dt(from_iso).astimezone(timezone.utc)
    dt_local = dt_utc.astimezone(tz)
    return dt_local.date()


def _planned_ms_moment_from_local_date(d: date) -> str:
    """
    МС требует moment. ТЗ: важна только дата, время любое.
    Ставим 00:00:00 UTC на нужную дату.
    """
    dt = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)
    # формат MS: "YYYY-MM-DD HH:MM:SS.mmm+0000"
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "+0000"


@dataclass(frozen=True)
class Cabinet:
    name: str
    client_id_env: str
    api_key_env: str
    sales_channel_id: str  # МС, кабинет-зависимый


@dataclass(frozen=True)
class CabinetRuntime:
    cabinet: Cabinet
    ozon: OzonClient


def _env_bool(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "y")


def sync_fbo_supplies(
    *,
    ms: MoySkladSupplyService,
    cabinets: List[CabinetRuntime],
) -> None:
    dry_run = _env_bool("FBO_DRY_RUN", "0")
    allow_delete = _env_bool("FBO_ALLOW_DELETE", "0")

    planned_from_str = os.environ.get("FBO_PLANNED_FROM", "2025-12-03").strip()  # YYYY-MM-DD
    planned_from = datetime.fromisoformat(planned_from_str).date()

    # состояния для листинга (чтобы не потерять ничего)
    list_states = [
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

    for c in cabinets:
        # важно: salesChannel зависит от кабинета
        ms.set_sales_channel(c.cabinet.sales_channel_id)

        order_ids = c.ozon.list_supply_order_ids(states=list_states)

        for oid in order_ids:
            core = c.ozon.get_supply_order(oid)

            order_number = str(core.get("order_number") or "").strip()
            if not order_number:
                continue

            # фильтр по плановой дате отгрузки (ключевое!)
            pd = _planned_local_date(core)
            if not pd:
                continue
            if pd < planned_from:
                continue

            state = str(core.get("state") or "").strip()

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

            # 3) получить позиции (offer_id + qty)
            items = c.ozon.get_supply_order_items(oid)  # <-- реализация в ozon_client должна вернуть список
            # ожидаемый формат: [{"offer_id":"...", "quantity": N}, ...]

            # 4) upsert CustomerOrder (всегда applicable=true)
            shipment_moment = _planned_ms_moment_from_local_date(pd)
            print(f"[{c.cabinet.name}] {order_number} upsert customerorder state={state}")
            co = ms.upsert_customerorder(
                order_number=order_number,
                shipment_planned_moment=shipment_moment,
                core=core,
                items=items,
                dry_run=dry_run,
            )
            if dry_run:
                continue

            # 5) upsert Move (только если demand нет — мы уже проверили)
            # move должен быть связан с заказом и пересобираться вместе с заказом
            mv, mv_has_positions = ms.upsert_move_linked_to_customerorder(
                order_number=order_number,
                customerorder_meta=co["meta"],
                core=core,
                items=items,
                dry_run=dry_run,
            )

            # 6) demand создаём только по нужным статусам и только если move реально есть и не пустой
            if state in STATE_NEED_DEMAND:
                if mv and mv_has_positions:
                    # externalCode для demand ставим = order_number (для поиска/идемпотентности),
                    # а name/номер оставляем как присвоит МС (хронология)
                    ms.ensure_demand_from_customerorder(
                        order_number=order_number,
                        customerorder_meta=co["meta"],
                        core=core,
                        dry_run=dry_run,
                    )
                    print(f"[{c.cabinet.name}] {order_number} demand ensured")
                else:
                    print(f"[{c.cabinet.name}] {order_number} demand skipped: no move/empty move")
