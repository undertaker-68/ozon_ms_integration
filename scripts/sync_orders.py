from __future__ import annotations

from datetime import datetime, timezone

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
            pn = p.get("posting_number")
            if not pn:
                continue

            d = oz.fbs_get(pn)
            r = d.get("result") or {}

            shipment_date = r.get("shipment_date")
            if not shipment_date:
                continue

            # фильтр по shipment_date (как договорились)
            if shipment_date < OZON_ORDERS_CUTOFF.isoformat().replace("+00:00", "Z"):
                continue

            order_number = r.get("order_number")
            status = r.get("status")
            products = r.get("products") or []

            if not order_number or not status:
                continue

            order = co.upsert_from_ozon(
                order_number=order_number,
                ozon_status=status,
                shipment_date=shipment_date,
                products=products,
                sales_channel_id=channel_id,
                posting_number=r.get("posting_number"),
            )

            status = (status or "").strip().lower()
            pn = r.get("posting_number") or ""

            if status == "delivering" and pn:
                dem.create_from_customerorder_if_missing(
                    customerorder=order,
                    posting_number=pn,
                    sales_channel_id=channel_id,
                )

            if status == "cancelled":
                co.remove_reserve(order)


            print(f"[{name}] synced order {order_number} status={status}")


if __name__ == "__main__":
    main()
