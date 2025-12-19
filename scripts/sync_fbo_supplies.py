from __future__ import annotations

import os

from app.supply_sync import sync_fbo_supplies, CabinetRuntime
from app.ozon_supply_client import OzonCabinet
from app.moysklad_supply_service import MsFboConfig


def _env_bool(name: str, default: str = "1") -> bool:
    v = os.environ.get(name, default).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def main() -> None:
    ms_token = os.environ["MOYSKLAD_TOKEN"]

    # из твоих данных (фикс)
    store_src_id = "7cdb9b20-9910-11ec-0a80-08670002d998"
    store_fbo_id = "77b4a517-3b82-11f0-0a80-18cb00037a24"
    state_customerorder_fbo_id = "921c872f-d54e-11ef-0a80-1823001350aa"
    state_move_supply_id = "b0d2c89d-5c7c-11ef-0a80-0cd4001f5885"
    state_demand_fbo_id = "b543e330-44e4-11f0-0a80-0da5002260ab"

    # эти у вас уже есть/должны быть (как в FBS)
    organization_id = os.environ["MS_ORGANIZATION_ID"]
    counterparty_ozon_id = os.environ["MS_COUNTERPARTY_OZON_ID"]

    base_url = os.environ.get("OZON_BASE_URL", "https://api-seller.ozon.ru")

    cab1 = OzonCabinet(
        name="ozon1",
        base_url=base_url,
        api_key=os.environ["OZON1_API_KEY"],
        client_id=os.environ["OZON1_CLIENT_ID"],
    )
    cab2 = OzonCabinet(
        name="ozon2",
        base_url=base_url,
        api_key=os.environ["OZON2_API_KEY"],
        client_id=os.environ["OZON2_CLIENT_ID"],
    )

    ms_cfg_1 = MsFboConfig(
        organization_id=organization_id,
        counterparty_ozon_id=counterparty_ozon_id,
        store_src_id=store_src_id,
        store_fbo_id=store_fbo_id,
        state_customerorder_fbo_id=state_customerorder_fbo_id,
        state_move_supply_id=state_move_supply_id,
        state_demand_fbo_id=state_demand_fbo_id,
        sales_channel_id="fede2826-9fd0-11ee-0a80-0641000f3d25",
        set_move_external_code=True,
    )
    ms_cfg_2 = MsFboConfig(
        organization_id=organization_id,
        counterparty_ozon_id=counterparty_ozon_id,
        store_src_id=store_src_id,
        store_fbo_id=store_fbo_id,
        state_customerorder_fbo_id=state_customerorder_fbo_id,
        state_move_supply_id=state_move_supply_id,
        state_demand_fbo_id=state_demand_fbo_id,
        sales_channel_id="ff2827b8-9fd0-11ee-0a80-0641000f3d31",
        set_move_external_code=True,
    )

    dry_run = _env_bool("FBO_DRY_RUN", "1")

    sync_fbo_supplies(
        ms_token=ms_token,
        cabinets=[
            CabinetRuntime(cabinet=cab1, ms_cfg=ms_cfg_1),
            CabinetRuntime(cabinet=cab2, ms_cfg=ms_cfg_2),
        ],
        created_from_iso="2025-12-03T00:00:00Z",
        dry_run=dry_run,
    )


if __name__ == "__main__":
    main()
