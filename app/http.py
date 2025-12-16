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
) -> Dict[str, Any]:
    # ВАЖНО: полностью игнорируем proxy из окружения
    s = requests.Session()
    s.trust_env = False

    r = s.request(
        method=method,
        url=url,
        headers=headers,
        params=params,
        json=json_body,
        timeout=timeout,
    )

    if r.status_code >= 400:
        raise HttpError(r.status_code, r.text, url)

    if not r.text.strip():
        return {}

    return r.json()
