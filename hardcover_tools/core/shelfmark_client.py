from __future__ import annotations

import http.cookiejar
import json
import socket
from dataclasses import dataclass
from typing import Any, Mapping, Optional
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request


DEFAULT_SHELFMARK_USER_AGENT = "hardcover-tools/shelfmark"


@dataclass(frozen=True)
class ShelfmarkApiError(RuntimeError):
    message: str
    status_code: int = 0
    response_body: str = ""
    payload: Optional[Mapping[str, Any]] = None
    kind: str = "http_error"
    retryable: bool = False

    def __str__(self) -> str:
        if self.status_code:
            return f"{self.message} (status={self.status_code}, kind={self.kind})"
        return self.message


class ShelfmarkClient:
    def __init__(
        self,
        *,
        base_url: str,
        timeout: int = 30,
        user_agent: str = DEFAULT_SHELFMARK_USER_AGENT,
    ) -> None:
        normalized_base = str(base_url or "").strip().rstrip("/")
        if not normalized_base:
            raise ValueError("Shelfmark base_url is required")
        self.base_url = normalized_base
        self.timeout = max(1, int(timeout))
        self.user_agent = str(user_agent or DEFAULT_SHELFMARK_USER_AGENT).strip() or DEFAULT_SHELFMARK_USER_AGENT
        self.cookie_jar = http.cookiejar.CookieJar()
        self.opener = urllib_request.build_opener(urllib_request.HTTPCookieProcessor(self.cookie_jar))

    def login(self, *, username: str, password: str, remember_me: bool = False) -> dict[str, Any]:
        response = self._request(
            "POST",
            "/api/auth/login",
            payload={
                "username": str(username or "").strip(),
                "password": str(password or ""),
                "remember_me": bool(remember_me),
            },
        )
        return dict(response) if isinstance(response, Mapping) else {}

    def get_request_policy(self) -> dict[str, Any]:
        payload = self._request("GET", "/api/request-policy")
        return dict(payload) if isinstance(payload, Mapping) else {}

    def create_request(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        response = self._request("POST", "/api/requests", payload=dict(payload))
        return dict(response) if isinstance(response, Mapping) else {}

    def search_releases(
        self,
        *,
        provider: Optional[str] = None,
        book_id: Optional[str] = None,
        source: Optional[str] = None,
        content_type: Optional[str] = None,
        query: Optional[str] = None,
        title: Optional[str] = None,
        author: Optional[str] = None,
        format_keywords: Optional[list[str]] = None,
        indexers: Optional[list[str]] = None,
        timeout_seconds: Optional[int] = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if provider:
            params["provider"] = str(provider).strip()
        if book_id:
            params["book_id"] = str(book_id).strip()
        if source:
            params["source"] = str(source).strip()
        if content_type:
            params["content_type"] = str(content_type).strip()
        if query:
            params["query"] = str(query).strip()
        if title:
            params["title"] = str(title).strip()
        if author:
            params["author"] = str(author).strip()
        if format_keywords:
            params["format"] = [str(keyword).strip() for keyword in format_keywords if str(keyword).strip()]
        if indexers:
            params["indexers"] = [str(indexer).strip() for indexer in indexers if str(indexer).strip()]
        payload = self._request("GET", "/api/releases", params=params, timeout_seconds=timeout_seconds)
        return dict(payload) if isinstance(payload, Mapping) else {}

    def queue_release(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        response = self._request("POST", "/api/releases/download", payload=dict(payload))
        return dict(response) if isinstance(response, Mapping) else {}

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        payload: Optional[Mapping[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
    ) -> Any:
        url = self._build_url(path, params=params)
        headers = {
            "Accept": "application/json",
            "User-Agent": self.user_agent,
        }
        data: bytes | None = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload).encode("utf-8")
        request = urllib_request.Request(url, data=data, headers=headers, method=method.upper())
        timeout = max(1, int(timeout_seconds or self.timeout))
        try:
            with self.opener.open(request, timeout=timeout) as response:
                raw_body = response.read().decode("utf-8")
                if not raw_body.strip():
                    return {}
                return json.loads(raw_body)
        except urllib_error.HTTPError as exc:
            raw_body = exc.read().decode("utf-8", errors="replace")
            parsed = _parse_error_payload(raw_body)
            raise ShelfmarkApiError(
                message=f"Shelfmark request failed for {method.upper()} {path}",
                status_code=int(getattr(exc, "code", 0) or 0),
                response_body=raw_body,
                payload=parsed,
                kind="http_error",
                retryable=_http_status_retryable(int(getattr(exc, "code", 0) or 0)),
            ) from exc
        except urllib_error.URLError as exc:
            reason = getattr(exc, "reason", exc)
            is_timeout = _looks_like_timeout(reason)
            raise ShelfmarkApiError(
                message=f"Shelfmark request failed for {method.upper()} {path}: {reason}",
                kind="timeout" if is_timeout else "transport_error",
                retryable=is_timeout,
            ) from exc
        except TimeoutError as exc:
            raise ShelfmarkApiError(
                message=f"Shelfmark request timed out for {method.upper()} {path}",
                kind="timeout",
                retryable=True,
            ) from exc
        except socket.timeout as exc:
            raise ShelfmarkApiError(
                message=f"Shelfmark request timed out for {method.upper()} {path}",
                kind="timeout",
                retryable=True,
            ) from exc
        except json.JSONDecodeError as exc:
            raise ShelfmarkApiError(
                message=f"Shelfmark returned non-JSON response for {method.upper()} {path}",
                kind="decode_error",
                retryable=False,
            ) from exc

    def _build_url(self, path: str, *, params: Optional[Mapping[str, Any]] = None) -> str:
        normalized_path = str(path or "").strip()
        if not normalized_path.startswith("/"):
            normalized_path = f"/{normalized_path}"
        url = self.base_url + normalized_path
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


def _looks_like_timeout(reason: Any) -> bool:
    if isinstance(reason, (TimeoutError, socket.timeout)):
        return True
    lowered = str(reason or "").strip().lower()
    return "timed out" in lowered or lowered == "timeout"


def _http_status_retryable(status_code: int) -> bool:
    return int(status_code or 0) in {408, 429, 500, 502, 503, 504}


__all__ = [
    "DEFAULT_SHELFMARK_USER_AGENT",
    "ShelfmarkApiError",
    "ShelfmarkClient",
]
