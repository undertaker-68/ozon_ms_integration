import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

def _req(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v.strip()

def _opt(name: str, default: str) -> str:
    return os.getenv(name, default).strip()

@dataclass(frozen=True)
class Config:
    moysklad_token: str
    moysklad_store_id: str

    ozon1_client_id: str
    ozon1_api_key: str
    ozon1_warehouse_id: int

    ozon2_client_id: str
    ozon2_api_key: str
    ozon2_warehouse_id: int

    log_level: str
    cache_dir: str

def load_config() -> Config:
    return Config(
        moysklad_token=_req("MOYSKLAD_TOKEN"),
        moysklad_store_id=_req("MOYSKLAD_STORE_ID"),

        ozon1_client_id=_req("OZON1_CLIENT_ID"),
        ozon1_api_key=_req("OZON1_API_KEY"),
        ozon1_warehouse_id=int(_req("OZON1_WAREHOUSE_ID")),

        ozon2_client_id=_req("OZON2_CLIENT_ID"),
        ozon2_api_key=_req("OZON2_API_KEY"),
        ozon2_warehouse_id=int(_req("OZON2_WAREHOUSE_ID")),

        log_level=_opt("LOG_LEVEL", "INFO").upper(),
        cache_dir=_opt("CACHE_DIR", "/var/tmp/ozon_ms_cache"),
    )
