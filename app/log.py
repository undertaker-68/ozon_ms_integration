import json
import logging
import sys
from datetime import datetime, timezone

def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        stream=sys.stdout,
        format="%(message)s",
    )

def log_json(logger: logging.Logger, msg: str, **fields) -> None:
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "msg": msg,
        **fields,
    }
    logger.info(json.dumps(payload, ensure_ascii=False))
