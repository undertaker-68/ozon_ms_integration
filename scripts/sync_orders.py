from __future__ import annotations

from datetime import datetime, timezone

from app.config import load_config
from app.moysklad_client import MoySkladClient
from app.ozon_client import OzonClient

from app.orders_sync.constants import (
    OZON_ORDERS_CUTOFF,
    MS_SALES_CHANNEL_CAB1_ID,
    MS_SALES_CHANNEL_CAB2_ID,
)
from app.orders_sync.ms_customerorder import CustomerOrderService

def now_utc():
    return datetime.now(timezone.utc)

def main():
    cfg = load_config()

    ms = MoySkladClient(cfg.moysklad_token)
    co = CustomerOrderService(ms)

    # ⚠️ Тут нужно как у вас в проекте хранятся 2 кабинета Ozon.
    # На первом шаге сделай просто два клиента вручную из env/конфига.
    accounts = [
        ("cab1", OzonClient(cfg.ozon_client_id_1, cfg.ozon_api_key_1), MS_SALES_CHANNEL_CAB1_ID),
        ("cab2", OzonClient(cfg.ozon_client_id_2, cfg.ozon_api_key_2), MS_SALES_CHANNEL_CAB2_ID),
    ]

    for name, oz, channel_id in accounts:
        # Базовый диапазон: cutoff -> now
        date_from = OZON_ORDERS_CUTOFF
        date_to = now_utc()

        postings = oz.fbs_list(date_from=date_from, date_to=date_to)  # сделаем в ozon_client
        for p in postings:
            # Детализация, чтобы точно иметь shipment_date/products/status/order_number
            d = oz.fbs_get(p["posting_number"])
            r = d["result"]

            created = r.get("in_process_at")  # не используем, но можно логировать
            shipment_date = r["shipment_date"]
            # отсечка по shipment_date/created? — по ТЗ "созданные ранее 08.12.25"
            # обычно лучше фильтровать по in_process_at или created_at, но в твоей таблице created_at нет.
            # Поэтому на первом шаге фильтруем по shipment_date (позже уточним если надо).
            if shipment_date < OZON_ORDERS_CUTOFF.isoformat().replace("+00:00", "Z"):
                continue

            order_number = r["order_number"]
            status = r["status"]
            products = r.get("products") or []

            order = co.upsert_from_ozon(
                order_number=order_number,
                ozon_status=status,
                shipment_date=shipment_date,
                products=products,
                sales_channel_id=channel_id,
                posting_number=r.get("posting_number"),
            )

            if status == "cancelled":
                co.remove_reserve(order)

            print(f"[{name}] synced order {order_number} status={status}")

if __name__ == "__main__":
    main()
