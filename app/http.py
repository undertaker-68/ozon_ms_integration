from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests


@dataclass
class HttpError(Exception):
    status: int
    text: str
    url: str

    def __str__(self) -> str:
        return f"HTTP {self.status} for {self.url}: {self.text}"


def _is_json(text: str) -> bool:
    t = (text or "").lstrip()
    return t.startswith("{") or t.startswith("[")


def request_json(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    json_body: Any = None,
    timeout: int = 60,
    retries: int = 6,
) -> Any:
    """
    Универсальный HTTP для проекта.

    Важно:
    - ретраи на 429 (MS rate limit) с экспоненциальным backoff
    - ретраи на сетевые таймауты/SSL handshake timeout
    - по умолчанию retries=6 достаточно для длинных прогонов
    """
    s = requests.Session()

    last_err: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            r = s.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=timeout,
            )

            # 429: ограничение запросов (часто у МС)
            if r.status_code == 429:
                # если MS отдает Retry-After — уважаем
                ra = r.headers.get("Retry-After")
                if ra:
                    try:
                        sleep_s = float(ra)
                    except Exception:
                        sleep_s = 2.0
                else:
                    sleep_s = min(30.0, 1.5 * (2**attempt))
                time.sleep(sleep_s)
                continue

            if r.status_code >= 400:
                raise HttpError(r.status_code, r.text, url)

            if not r.text:
                return None
            if _is_json(r.text):
                return r.json()
            # иногда МС/Озон могут вернуть text/plain
            return r.text

        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout) as e:
            last_err = e
            # backoff
            time.sleep(min(30.0, 1.5 * (2**attempt)))
            continue
        except requests.exceptions.SSLError as e:
            last_err = e
            time.sleep(min(30.0, 1.5 * (2**attempt)))
            continue
        except requests.exceptions.ConnectionError as e:
            last_err = e
            time.sleep(min(30.0, 1.5 * (2**attempt)))
            continue
        except HttpError as e:
            # 5xx можно ретраить, остальное — нет
            if 500 <= e.status < 600 and attempt < retries:
                last_err = e
                time.sleep(min(30.0, 1.5 * (2**attempt)))
                continue
            raise

    # если сюда дошли — все ретраи исчерпаны
    if last_err:
        raise last_err
    raise RuntimeError(f"request_json failed for {url}")
