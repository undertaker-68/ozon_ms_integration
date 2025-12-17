from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

OZON_ORDERS_CUTOFF = datetime(2025, 12, 8, 0, 0, 0, tzinfo=timezone.utc)

MS_COUNTERPARTY_OZON_ID = "f61bfcf9-2d74-11ec-0a80-04c700041e03"
MS_STORE_OZON_ID = "42db7535-5bb6-11ef-0a80-1589000daaa3"

# CustomerOrder states (MS)
OZON_TO_MS_STATE = {
    "awaiting_packaging": "ffb88772-9fd0-11ee-0a80-0641000f3d5f",
    "awaiting_deliver":   "ffbc9d6b-9fd0-11ee-0a80-0641000f3d62",
    "delivering":         "ffbe5466-9fd0-11ee-0a80-0641000f3d64",
    "delivered":          "ffc02196-9fd0-11ee-0a80-0641000f3d66",
    "cancelled":          "ffc1c72c-9fd0-11ee-0a80-0641000f3d68",
}

# Demand state FBS (MS)
MS_DEMAND_STATE_FBS_ID = "b543df0c-44e4-11f0-0a80-0da5002260aa"

# Sales channels
MS_SALES_CHANNEL_CAB1_ID = "fede2826-9fd0-11ee-0a80-0641000f3d25"
MS_SALES_CHANNEL_CAB2_ID = "ff2827b8-9fd0-11ee-0a80-0641000f3d31"
