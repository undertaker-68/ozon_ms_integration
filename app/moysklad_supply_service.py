from __future__ import annotations

import os

from app.supply_sync import sync_fbo_supplies, CabinetRuntime
from app.ozon_supply_client import OzonCabinet, OzonSupplyClient
from app.moysklad_supply_service import MsFboConfig, MoySkladSupplyService


def _env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if v is None or str(v).strip() == "":
        raise KeyError(name)
    return str(v).strip()


def main() -> None:
    # MS token
    ms_token = _env("MOYSKLAD_TOKEN")

    # MS: org + counterparty
    organization_id = _env("MS_ORGANIZATION_ID", os.environ.get("MOYSKLAD_ORG_ID"))
    counterparty_ozon_id = _env("MS_COUNTERPARTY_OZON_ID", os.environ.get("MOYSKLAD_OZON_COUNTERPARTY_ID"))

    # Склады (если нет MS_STORE_SRC_ID/MS_STORE_FBO_ID — берём известные)
    # источник (СКЛАД)
    store_src_id = os.environ.get("MS_STORE_SRC_ID") or os.environ.get("MOYSKLAD_STORE_ID") or "7cdb9b20-9910-11ec-0a80-08670002d998"
    store_src_id = str(store_src_id).strip()

    # назначение (FBO)
    store_fbo_id = os.environ.get("MS_STORE_FBO_ID") or "77b4a517-3b82-11f0-0a80-18cb00037a24"
    store_fbo_id = str(store_fbo_id).strip()

    # Статусы
    state_customerorder_fbo_id = _env("MS_STATE_CUSTOMERORDER_FBO_ID")
    state_move_supply_id = _env("MS_STATE_MOVE_SUPPLY_ID")
    state_demand_fbo_id = _env("MS_STATE_DEMAND_FBO_ID")

    base_url = os.environ.get("OZON_BASE_URL") or "https://api-seller.ozon.ru"

    cabinets: list[CabinetRuntime] = []

    # ===== Кабинет 1 =====
    cab1 = OzonCabinet(
        name="ozon1",
        base_url=base_url,
        api_key=_env("OZON1_API_KEY"),
        client_id=_env("OZON1_CLIENT_ID"),
    )
    oz1 = OzonSupplyClient(cabinet=cab1)

    ms_cfg_1 = MsFboConfig(
        organization_id=organization_id,
        counterparty_ozon_id=counterparty_ozon_id,
        store_src_id=store_src_id,
        store_fbo_id=store_fbo_id,
        state_customerorder_fbo_id=state_customerorder_fbo_id,
        state_move_supply_id=state_move_supply_id,
        state_demand_fbo_id=state_demand_fbo_id,
        sales_channel_id=_env("MS_SALES_CHANNEL_OZON1"),
    )
    ms1 = MoySkladSupplyService(ms_token=ms_token, cfg=ms_cfg_1)

    cabinets.append(CabinetRuntime(name="ozon1", ozon=oz1, ms=ms1))

    # ===== Кабинет 2 (опционально) =====
    ozon2_client_id = os.environ.get("OZON2_CLIENT_ID")
    ozon2_api_key = os.environ.get("OZON2_API_KEY")
    if ozon2_client_id and ozon2_api_key:
        cab2 = OzonCabinet(
            name="ozon2",
            base_url=base_url,
            api_key=_env("OZON2_API_KEY"),
            client_id=_env("OZON2_CLIENT_ID"),
        )
        oz2 = OzonSupplyClient(cabinet=cab2)

        ms_cfg_2 = MsFboConfig(
            organization_id=organization_id,
            counterparty_ozon_id=counterparty_ozon_id,
            store_src_id=store_src_id,
            store_fbo_id=store_fbo_id,
            state_customerorder_fbo_id=state_customerorder_fbo_id,
            state_move_supply_id=state_move_supply_id,
            state_demand_fbo_id=state_demand_fbo_id,
            sales_channel_id=_env("MS_SALES_CHANNEL_OZON2"),
        )
        ms2 = MoySkladSupplyService(ms_token=ms_token, cfg=ms_cfg_2)

        cabinets.append(CabinetRuntime(name="ozon2", ozon=oz2, ms=ms2))

    sync_fbo_supplies(ms_token=ms_token, cabinets=cabinets)


if __name__ == "__main__":
    main()
