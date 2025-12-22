from __future__ import annotations

import os

from app.supply_sync import sync_fbo_supplies, CabinetRuntime
from app.ozon_supply_client import OzonCabinet
from app.moysklad_supply_service import MsFboConfig


def _env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if v is None or str(v).strip() == "":
        raise KeyError(name)
    return str(v).strip()


def main() -> None:
    # Токен МС (у тебя он точно есть)
    ms_token = _env("MOYSKLAD_TOKEN")

    base_url = os.environ.get("OZON_BASE_URL", "https://api-seller.ozon.ru").strip()

    # MS IDs (общее)
    organization_id = os.environ.get("MS_ORGANIZATION_ID") or os.environ.get("MOYSKLAD_ORG_ID")
    counterparty_ozon_id = os.environ.get("MS_COUNTERPARTY_OZON_ID") or os.environ.get("MOYSKLAD_OZON_COUNTERPARTY_ID")
    if not organization_id:
        raise KeyError("MS_ORGANIZATION_ID (или MOYSKLAD_ORG_ID)")
    if not counterparty_ozon_id:
        raise KeyError("MS_COUNTERPARTY_OZON_ID (или MOYSKLAD_OZON_COUNTERPARTY_ID)")

    # Склады:
    # - источник: MS_STORE_SRC_ID (если нет — попробуем MOYSKLAD_STORE_ID как fallback)
    # - FBO: MS_STORE_FBO_ID (обязателен)
    store_src_id = os.environ.get("MS_STORE_SRC_ID") or os.environ.get("MOYSKLAD_STORE_ID")
    store_fbo_id = os.environ.get("MS_STORE_FBO_ID")
    if not store_src_id:
        raise KeyError("MS_STORE_SRC_ID (или MOYSKLAD_STORE_ID как fallback)")
    if not store_fbo_id:
        raise KeyError("MS_STORE_FBO_ID")

    # Статусы (у тебя есть)
    state_customerorder_fbo_id = _env("MS_STATE_CUSTOMERORDER_FBO_ID")
    state_move_supply_id = _env("MS_STATE_MOVE_SUPPLY_ID")
    state_demand_fbo_id = _env("MS_STATE_DEMAND_FBO_ID")

    # Кабинет 1
    cab1 = OzonCabinet(
        name="ozon1",
        base_url=base_url,
        api_key=_env("OZON1_API_KEY"),
        client_id=_env("OZON1_CLIENT_ID"),
    )
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

    cabinets = [CabinetRuntime(cabinet=cab1, ms_cfg=ms_cfg_1)]

    # Кабинет 2 (опционально)
    if os.environ.get("OZON2_API_KEY") and os.environ.get("OZON2_CLIENT_ID") and os.environ.get("MS_SALES_CHANNEL_OZON2"):
        cab2 = OzonCabinet(
            name="ozon2",
            base_url=base_url,
            api_key=_env("OZON2_API_KEY"),
            client_id=_env("OZON2_CLIENT_ID"),
        )
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
        cabinets.append(CabinetRuntime(cabinet=cab2, ms_cfg=ms_cfg_2))

    sync_fbo_supplies(ms_token=ms_token, cabinets=cabinets)


if __name__ == "__main__":
    main()
