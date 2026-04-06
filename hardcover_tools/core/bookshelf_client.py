from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Sequence
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request


DEFAULT_BOOKSHELF_USER_AGENT = "hardcover-tools/bookshelf"


@dataclass(frozen=True)
class BookshelfApiError(RuntimeError):
    message: str
    status_code: int = 0
    response_body: str = ""
    payload: Optional[Mapping[str, Any]] = None

    def __str__(self) -> str:
        if self.status_code:
            return f"{self.message} (status={self.status_code})"
        return self.message


class BookshelfClient:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        timeout: int = 30,
        user_agent: str = DEFAULT_BOOKSHELF_USER_AGENT,
    ) -> None:
        normalized_base = str(base_url or "").strip().rstrip("/")
        if not normalized_base:
            raise ValueError("Bookshelf base_url is required")
        self.base_url = normalized_base
        self.api_key = str(api_key or "").strip()
        if not self.api_key:
            raise ValueError("Bookshelf api_key is required")
        self.timeout = max(1, int(timeout))
        self.user_agent = str(user_agent or DEFAULT_BOOKSHELF_USER_AGENT).strip() or DEFAULT_BOOKSHELF_USER_AGENT

    def get_development_config(self) -> Dict[str, Any]:
        payload = self._request("GET", "/api/v1/config/development")
        return dict(payload) if isinstance(payload, Mapping) else {}

    def search(self, term: str) -> list[dict[str, Any]]:
        payload = self._request("GET", "/api/v1/search", params={"term": str(term or "").strip()})
        if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
            return [dict(item) for item in payload if isinstance(item, Mapping)]
        return []

    def add_book(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        response = self._request("POST", "/api/v1/book", payload=dict(payload))
        return dict(response) if isinstance(response, Mapping) else {}

    def add_author(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        response = self._request("POST", "/api/v1/author", payload=dict(payload))
        return dict(response) if isinstance(response, Mapping) else {}

    def enqueue_command(self, name: str, body: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
        payload: dict[str, Any] = {"name": str(name or "").strip()}
        if body:
            payload.update(dict(body))
        response = self._request("POST", "/api/v1/command", payload=payload)
        return dict(response) if isinstance(response, Mapping) else {}

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        url = self._build_url(path, params=params)
        headers = {
            "Accept": "application/json",
            "User-Agent": self.user_agent,
            "X-Api-Key": self.api_key,
        }
        data: bytes | None = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload).encode("utf-8")
        request = urllib_request.Request(url, data=data, headers=headers, method=method.upper())
        try:
            with urllib_request.urlopen(request, timeout=self.timeout) as response:
                raw_body = response.read().decode("utf-8")
                if not raw_body.strip():
                    return {}
                return json.loads(raw_body)
        except urllib_error.HTTPError as exc:
            raw_body = exc.read().decode("utf-8", errors="replace")
            parsed = _parse_error_payload(raw_body)
            raise BookshelfApiError(
                message=f"Bookshelf request failed for {method.upper()} {path}",
                status_code=int(getattr(exc, "code", 0) or 0),
                response_body=raw_body,
                payload=parsed,
            ) from exc
        except urllib_error.URLError as exc:
            raise BookshelfApiError(message=f"Bookshelf request failed for {method.upper()} {path}: {exc.reason}") from exc
        except json.JSONDecodeError as exc:
            raise BookshelfApiError(message=f"Bookshelf returned non-JSON response for {method.upper()} {path}") from exc

    def _build_url(self, path: str, *, params: Optional[Mapping[str, Any]] = None) -> str:
        normalized_path = str(path or "").strip()
        if not normalized_path.startswith("/"):
            normalized_path = f"/{normalized_path}"
        base = self.base_url
        if base.endswith("/api/v1") and normalized_path.startswith("/api/v1/"):
            url = base + normalized_path[len("/api/v1") :]
        elif base.endswith("/api/v1") and normalized_path == "/api/v1":
            url = base
        else:
            url = base + normalized_path
        if params:
            encoded = urllib_parse.urlencode(
                {key: value for key, value in params.items() if value not in (None, "")},
                doseq=True,
            )
            if encoded:
                return f"{url}?{encoded}"
        return url


def _parse_error_payload(raw_body: str) -> Optional[Mapping[str, Any]]:
    body = str(raw_body or "").strip()
    if not body:
        return None
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, Mapping) else None


__all__ = [
    "BookshelfApiError",
    "BookshelfClient",
    "DEFAULT_BOOKSHELF_USER_AGENT",
]
