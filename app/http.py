import time
import requests
from typing import Any, Dict, Optional

class HttpError(RuntimeError):
    def __init__(self, status: int, text: str, url: str):
        super().__init__(f"HTTP {status} for {url}: {text[:500]}")
        self.status = status
        self.text = text
        self.url = url

def request_json(
    method: str,
    url: str,
    headers: Dict[str, str],
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: int = 60,
    max_retries: int = 6,
) -> Dict[str, Any]:
    # ВАЖНО: игнорируем proxy из окружения
    s = requests.Session()
    s.trust_env = False

    for attempt in range(max_retries + 1):
        r = s.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            json=json_body,
            timeout=timeout,
        )

        # Retry on rate limit / temporary errors
        if r.status_code in (429, 502, 503, 504) and attempt < max_retries:
            ra = r.headers.get("Retry-After")
            if ra and ra.isdigit():
                sleep_s = int(ra)
            else:
                # exponential backoff: 1,2,4,8... with small cap
                sleep_s = min(20, 2 ** attempt)
            time.sleep(sleep_s)
            continue

        if r.status_code >= 400:
            raise HttpError(r.status_code, r.text, url)

        if not r.text.strip():
            return {}

        return r.json()

    # theoretically unreachable
    raise HttpError(599, "Max retries exceeded", url)
