from __future__ import annotations

from datetime import datetime, timezone

import requests

from app.config import load_config
from app.moysklad_client import MoySkladClient
from app.ozon_client import OzonClient, OzonCreds

from app.orders_sync.constants import (
    OZON_ORDERS_CUTOFF,
    MS_SALES_CHANNEL_CAB1_ID,
    MS_SALES_CHANNEL_CAB2_ID,
)
from app.orders_sync.ms_customerorder import CustomerOrderService
from app.orders_sync.ms_demand import DemandService

SHIPMENT_DATE_FROM = datetime(2025, 12, 3, tzinfo=timezone.utc)  # 03.12.2025 включительно

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def main() -> None:
    cfg = load_config()

    ms = MoySkladClient(cfg.moysklad_token)
    co = CustomerOrderService(ms)
    dem = DemandService(ms)

    accounts = [
        (
            "cab1",
            OzonClient(
                OzonCreds(
                    name="cab1",
                    client_id=cfg.ozon1_client_id,
                    api_key=cfg.ozon1_api_key,
                    warehouse_id=cfg.ozon1_warehouse_id,
                ),
                cache_dir=cfg.cache_dir,
            ),
            MS_SALES_CHANNEL_CAB1_ID,
        ),
        (
            "cab2",
            OzonClient(
                OzonCreds(
                    name="cab2",
                    client_id=cfg.ozon2_client_id,
                    api_key=cfg.ozon2_api_key,
                    warehouse_id=cfg.ozon2_warehouse_id,
                ),
                cache_dir=cfg.cache_dir,
            ),
            MS_SALES_CHANNEL_CAB2_ID,
        ),
    ]

    date_from = OZON_ORDERS_CUTOFF
    date_to = now_utc()

    for name, oz, channel_id in accounts:
        postings = oz.fbs_list(date_from=date_from, date_to=date_to, limit=100)

        for p in postings:
            posting_number = p.get("posting_number")
            if not posting_number:
                continue

            d = oz.fbs_get(posting_number)
            r = d.get("result") or {}

            posting_number = (r.get("posting_number") or "").strip()
            status = (r.get("status") or "").strip().lower()
            shipment_date = r.get("shipment_date")
            products = r.get("products") or []

            if not posting_number or not status or not shipment_date:
                continue

            # фильтр по дате отгрузки (shipment_date) — берём только с 03.12.2025 включительно
            try:
                sd = datetime.fromisoformat(shipment_date.replace("Z", "+00:00"))
            except Exception:
                continue

            if sd < SHIPMENT_DATE_FROM:
                continue

            try:
                order = co.upsert_from_ozon(
                    order_number=posting_number,      # ключ МС = posting_number
                    ozon_status=status,
                    shipment_date=shipment_date,
                    products=products,
                    sales_channel_id=channel_id,
                    posting_number=posting_number,
                )
            except Exception as e:
                print(f"[{name}] SKIP posting {posting_number}: {e}")
                continue

            # подчистить дубли отгрузок по связанному заказу (если они уже есть)
            co.ensure_prices(order)
            try:
                dem.ensure_single_demand_for_order(order)
            except requests.exceptions.RequestException as e:
                print(f"[{name}] WARN MS request failed (ensure_single_demand_for_order) {posting_number}: {e}")

            # delivering → создаём отгрузку (если нет)
            if status == "delivering":
                try:
                    demand = dem.create_from_customerorder_if_missing(
                        customerorder=order,
                        posting_number=posting_number,
                        sales_channel_id=channel_id,
                    )
                    if demand is None:
                        print(f"[{name}] SKIP demand for {posting_number}: no stock in MS")
                except requests.exceptions.RequestException as e:
                    print(f"[{name}] WARN MS request failed (create demand) {posting_number}: {e}")

            # cancelled → снимаем резерв
            if status == "cancelled":
                co.remove_reserve(order)

            print(f"[{name}] synced {posting_number} status={status}")


if __name__ == "__main__":
    main()
