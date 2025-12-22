from __future__ import annotations

import os

from app.supply_sync import sync_fbo_supplies, CabinetRuntime
from app.ozon_supply_client import OzonCabinet
from app.moysklad_supply_service import MsFboConfig


def main() -> None:
    ms_token = os.environ["MOYSKLAD_TOKEN"]

    base_url = os.environ.get("OZON_BASE_URL", "https://api-seller.ozon.ru").strip()

    # MS IDs (общее)
    store_src_id = os.environ["MS_STORE_SRC_ID"]
    store_fbo_id = os.environ["MS_STORE_FBO_ID"]
    state_customerorder_fbo_id = os.environ["MS_STATE_CUSTOMERORDER_FBO_ID"]
    state_move_supply_id = os.environ["MS_STATE_MOVE_SUPPLY_ID"]
    state_demand_fbo_id = os.environ["MS_STATE_DEMAND_FBO_ID"]
    organization_id = os.environ["MS_ORGANIZATION_ID"]
    counterparty_ozon_id = os.environ["MS_COUNTERPARTY_OZON_ID"]

    # Кабинет 1
    cab1 = OzonCabinet(
        name="ozon1",
        base_url=base_url,
        api_key=os.environ["OZON1_API_KEY"],
        client_id=os.environ["OZON1_CLIENT_ID"],
    )
    ms_cfg_1 = MsFboConfig(
        organization_id=organization_id,
        counterparty_ozon_id=counterparty_ozon_id,
        store_src_id=store_src_id,
        store_fbo_id=store_fbo_id,
        state_customerorder_fbo_id=state_customerorder_fbo_id,
        state_move_supply_id=state_move_supply_id,
        state_demand_fbo_id=state_demand_fbo_id,
        sales_channel_id=os.environ["MS_SALES_CHANNEL_OZON1"],
    )

    cabinets = [CabinetRuntime(cabinet=cab1, ms_cfg=ms_cfg_1)]

    # Кабинет 2 (опционально)
    if os.environ.get("OZON2_API_KEY") and os.environ.get("OZON2_CLIENT_ID") and os.environ.get("MS_SALES_CHANNEL_OZON2"):
        cab2 = OzonCabinet(
            name="ozon2",
            base_url=base_url,
            api_key=os.environ["OZON2_API_KEY"],
            client_id=os.environ["OZON2_CLIENT_ID"],
        )
        ms_cfg_2 = MsFboConfig(
            organization_id=organization_id,
            counterparty_ozon_id=counterparty_ozon_id,
            store_src_id=store_src_id,
            store_fbo_id=store_fbo_id,
            state_customerorder_fbo_id=state_customerorder_fbo_id,
            state_move_supply_id=state_move_supply_id,
            state_demand_fbo_id=state_demand_fbo_id,
            sales_channel_id=os.environ["MS_SALES_CHANNEL_OZON2"],
        )
        cabinets.append(CabinetRuntime(cabinet=cab2, ms_cfg=ms_cfg_2))

    sync_fbo_supplies(ms_token=ms_token, cabinets=cabinets)


if __name__ == "__main__":
    main()
