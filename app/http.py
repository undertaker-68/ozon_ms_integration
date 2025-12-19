cat > /root/ozon_ms_integration/app/http.py <<'PY'
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests


@dataclass
class HttpError(RuntimeError):
    status_code: int
    text: str
    url: str

    def __str__(self) -> str:
        return f"HTTP {self.status_code} for {self.url}: {self.text}"


# Один Session на весь процесс = меньше SSL handshakes, лучше keep-alive
_SESSION = requests.Session()


def request_json(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    json_body: Any = None,
    timeout: int = 60,
    retries: int = 4,
    backoff: float = 2.0,
) -> Any:
    """
    Универсальный запрос с ретраями.
    Ловим сетевые/таймаутные ошибки, которые часто бывают у api.moysklad.ru.
    """
    last_exc: Optional[BaseException] = None

    for attempt in range(1, retries + 1):
        try:
            r = _SESSION.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=timeout,
            )

            if r.status_code >= 400:
                raise HttpError(r.status_code, r.text, url)

            # иногда API может вернуть пустое тело на DELETE
            if not r.text:
                return {}

            try:
                return r.json()
            except json.JSONDecodeError:
                # если вдруг пришло не-json
                return r.text

        except (requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectTimeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.SSLError) as e:
            last_exc = e
            if attempt >= retries:
                raise

            # backoff: 2s, 4s, 8s ...
            sleep_s = backoff ** (attempt - 1)
            print(f"[http] retry {attempt}/{retries} {method} {url} due to {type(e).__name__} (sleep {sleep_s:.0f}s)")
            time.sleep(sleep_s)

    # на практике не дойдёт, но пусть будет
    if last_exc:
        raise last_exc
    raise RuntimeError("request_json failed without exception")
PY
