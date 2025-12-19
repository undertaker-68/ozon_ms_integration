import os

from app.supply_sync import sync_fbo_supplies, Cabinet, CabinetRuntime
from app.ozon_client import OzonClient
from app.moysklad_supply_service import MoySkladSupplyService, MsFboConfig


def main() -> None:
    ms_token = os.environ["MOYSKLAD_TOKEN"]

    # MS IDs (из твоих сообщений)
    cfg = MsFboConfig(
        organization_id=os.environ["MOYSKLAD_ORG_ID"],
        counterparty_ozon_id=os.environ["MOYSKLAD_OZON_COUNTERPARTY_ID"],
        store_src_id="7cdb9b20-9910-11ec-0a80-08670002d998",        # СКЛАД
        store_fbo_id="77b4a517-3b82-11f0-0a80-18cb00037a24",        # FBO
        state_customerorder_fbo_id="921c872f-d54e-11ef-0a80-1823001350aa",
        state_move_supply_id="b0d2c89d-5c7c-11ef-0a80-0cd4001f5885",
        state_demand_fbo_id="b543e330-44e4-11f0-0a80-0da5002260ab",
        sales_channel_id="fede2826-9fd0-11ee-0a80-0641000f3d25",    # будет переопределяться на кабинетный
    )

    ms = MoySkladSupplyService(ms_token=ms_token, cfg=cfg)

    # Кабинеты + их salesChannelId (МС)
    cab1 = Cabinet(
        name="ozon1",
        client_id_env="OZON1_CLIENT_ID",
        api_key_env="OZON1_API_KEY",
        sales_channel_id="fede2826-9fd0-11ee-0a80-0641000f3d25",
    )
    cab2 = Cabinet(
        name="ozon2",
        client_id_env="OZON2_CLIENT_ID",
        api_key_env="OZON2_API_KEY",
        sales_channel_id="ff2827b8-9fd0-11ee-0a80-0641000f3d31",
    )

    cabinets = []
    for cab in (cab1, cab2):
        client_id = os.environ.get(cab.client_id_env, "").strip()
        api_key = os.environ.get(cab.api_key_env, "").strip()
        if not client_id or not api_key:
            print(f"[{cab.name}] skip: missing env {cab.client_id_env}/{cab.api_key_env}")
            continue

        oz = OzonClient(client_id=client_id, api_key=api_key)
        cabinets.append(CabinetRuntime(cabinet=cab, ozon=oz))

    if not cabinets:
        print("No cabinets configured (missing OZON*_CLIENT_ID/OZON*_API_KEY).")
        return

    sync_fbo_supplies(ms=ms, cabinets=cabinets)


if __name__ == "__main__":
    main()
