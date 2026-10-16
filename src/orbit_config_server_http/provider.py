# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Bounded HTTPS polling adapter for Orbit's remote configuration contract."""

from __future__ import annotations

import asyncio
import json
import math
import random
import re
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from urllib.parse import quote, urlsplit

import httpx
from orbit_config_server import MAX_REMOTE_CONFIG_BYTES, RemoteConfigSnapshot
from orbit_config_server.contracts import RemoteConfigurationError
from pydantic import SecretStr

_SECTION = re.compile(r"[a-z][a-z0-9_.-]{0,62}\Z")
_MAX_RESPONSE_BYTES = MAX_REMOTE_CONFIG_BYTES + 16_384


class HTTPConfigurationProviderError(RemoteConfigurationError):
    """Sanitized transport or wire-protocol error from the HTTP provider."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        """Expose only a fixed safe message and an optional HTTP status code."""
        self.status_code = status_code
        super().__init__(message)


class HTTPRemoteConfigurationProvider(
    AbstractAsyncContextManager["HTTPRemoteConfigurationProvider"]
):
    """Poll one named configuration section from a versioned HTTP endpoint.

    The remote endpoint returns ``{"version": positive_integer, "config": object}`` as JSON
    from ``GET /v1/config/{section}``. Each result is a complete replacement. The provider owns
    its HTTPX client and closes it through :meth:`aclose` or the async context manager. HTTPS is
    required unless ``allow_insecure_http`` is explicitly enabled for local development.
    """

    def __init__(
        self,
        base_url: str,
        section: str,
        *,
        bearer_token: str | None = None,
        timeout: float = 5.0,
        poll_interval: float = 1.0,
        poll_jitter: float = 0.1,
        allow_insecure_http: bool = False,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """Validate endpoint policy and construct a bounded single-purpose HTTP client."""
        if not isinstance(base_url, str) or not base_url or len(base_url) > 2_048:
            raise ValueError("base_url must be a bounded absolute URL.")
        if not isinstance(section, str) or _SECTION.fullmatch(section) is None:
            raise ValueError("section must be a lowercase portable identifier.")
        if not isinstance(allow_insecure_http, bool):
            raise TypeError("allow_insecure_http must be a boolean.")
        try:
            parsed = urlsplit(base_url)
            hostname = parsed.hostname
            _ = parsed.port
        except ValueError:
            raise ValueError("base_url must be a valid absolute URL.") from None
        allowed_schemes = {"https", "http"} if allow_insecure_http else {"https"}
        if (
            parsed.scheme.lower() not in allowed_schemes
            or not hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "base_url must use HTTPS and must not contain credentials or query data."
            )
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
            raise TypeError("timeout must be a number.")
        if not math.isfinite(timeout) or not 0.1 <= timeout <= 60:
            raise ValueError("timeout must be between 0.1 and 60 seconds.")
        if isinstance(poll_interval, bool) or not isinstance(poll_interval, (int, float)):
            raise TypeError("poll_interval must be a number.")
        if not math.isfinite(poll_interval) or not 0.1 <= poll_interval <= 300:
            raise ValueError("poll_interval must be between 0.1 and 300 seconds.")
        if isinstance(poll_jitter, bool) or not isinstance(poll_jitter, (int, float)):
            raise TypeError("poll_jitter must be a number.")
        if not math.isfinite(poll_jitter) or not 0 <= poll_jitter <= 0.5:
            raise ValueError("poll_jitter must be between zero and 0.5.")
        if bearer_token is not None and (
            not isinstance(bearer_token, str)
            or not bearer_token
            or len(bearer_token) > 8_192
            or any(ord(character) < 33 or ord(character) > 126 for character in bearer_token)
        ):
            raise ValueError("bearer_token must be bounded printable text.")

        self._url = f"{base_url.rstrip('/')}/v1/config/{quote(section, safe='')}"
        self._token = None if bearer_token is None else SecretStr(bearer_token)
        self._poll_interval = float(poll_interval)
        self._poll_jitter = float(poll_jitter)
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(float(timeout)),
            follow_redirects=False,
            trust_env=False,
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
            transport=transport,
        )
        self._fetch_lock = asyncio.Lock()
        self._latest: RemoteConfigSnapshot | None = None
        self._closed = False

    async def __aenter__(self) -> HTTPRemoteConfigurationProvider:
        """Return this provider while its owned HTTP client remains open."""
        self._ensure_open()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Close the owned HTTP client when leaving the async context."""
        await self.aclose()

    async def fetch(self) -> RemoteConfigSnapshot:
        """Fetch, validate, and return one complete versioned JSON configuration snapshot."""
        self._ensure_open()
        async with self._fetch_lock:
            headers = {"accept": "application/json"}
            if self._token is not None:
                headers["authorization"] = f"Bearer {self._token.get_secret_value()}"
            try:
                async with self._client.stream("GET", self._url, headers=headers) as response:
                    if response.status_code != 200:
                        raise HTTPConfigurationProviderError(
                            "Remote configuration endpoint returned an unsuccessful status.",
                            status_code=response.status_code,
                        )
                    content_type = response.headers.get("content-type", "").split(";", 1)[0]
                    if not _is_json_media_type(content_type.strip().lower()):
                        raise HTTPConfigurationProviderError(
                            "Remote configuration endpoint returned an unsupported media type."
                        )
                    content_length = response.headers.get("content-length")
                    if content_length is not None and (
                        not content_length.isdecimal() or int(content_length) > _MAX_RESPONSE_BYTES
                    ):
                        raise HTTPConfigurationProviderError(
                            "Remote configuration response exceeds its size limit."
                        )
                    chunks: list[bytes] = []
                    total = 0
                    async for chunk in response.aiter_bytes():
                        total += len(chunk)
                        if total > _MAX_RESPONSE_BYTES:
                            raise HTTPConfigurationProviderError(
                                "Remote configuration response exceeds its size limit."
                            )
                        chunks.append(chunk)
            except asyncio.CancelledError:
                raise
            except HTTPConfigurationProviderError:
                raise
            except httpx.TimeoutException:
                raise HTTPConfigurationProviderError(
                    "Remote configuration request timed out."
                ) from None
            except httpx.HTTPError:
                raise HTTPConfigurationProviderError(
                    "Remote configuration request failed."
                ) from None

            version, config = _decode_response(b"".join(chunks))
            payload = _encode_config(config)
            snapshot = RemoteConfigSnapshot(version=version, payload=payload)
            if self._latest is not None:
                if snapshot.version < self._latest.version:
                    raise HTTPConfigurationProviderError(
                        "Remote configuration version moved backwards."
                    )
                if (
                    snapshot.version == self._latest.version
                    and snapshot.payload != self._latest.payload
                ):
                    raise HTTPConfigurationProviderError(
                        "Remote configuration reused a version for different values."
                    )
            self._latest = snapshot
            return snapshot

    async def watch(self, *, after_version: int) -> AsyncIterator[RemoteConfigSnapshot]:
        """Poll for complete snapshots whose versions are newer than ``after_version``."""
        if (
            isinstance(after_version, bool)
            or not isinstance(after_version, int)
            or after_version < 0
        ):
            raise ValueError("after_version must be a nonnegative integer.")
        version = after_version
        while True:
            snapshot = await self.fetch()
            if snapshot.version < version:
                raise HTTPConfigurationProviderError(
                    "Remote configuration version is older than the requested cursor."
                )
            if snapshot.version > version:
                version = snapshot.version
                yield snapshot
            delay = self._poll_interval * random.uniform(
                1 - self._poll_jitter, 1 + self._poll_jitter
            )
            await asyncio.sleep(delay)

    async def aclose(self) -> None:
        """Close the provider-owned HTTP client; repeated calls are safe."""
        if self._closed:
            return
        self._closed = True
        await self._client.aclose()

    def _ensure_open(self) -> None:
        """Reject new operations after this provider has been closed."""
        if self._closed:
            raise HTTPConfigurationProviderError("Remote configuration provider is closed.")


def _is_json_media_type(value: str) -> bool:
    """Accept standard JSON and structured-suffix JSON media types."""
    return value == "application/json" or (
        value.startswith("application/") and value.endswith("+json")
    )


def _decode_response(payload: bytes) -> tuple[int, object]:
    """Parse a bounded, duplicate-free versioned JSON response without echoing its values."""
    try:
        value = json.loads(
            payload.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (json.JSONDecodeError, RecursionError, UnicodeDecodeError, ValueError):
        raise HTTPConfigurationProviderError(
            "Remote configuration response is invalid JSON."
        ) from None
    if (
        not isinstance(value, dict)
        or set(value) != {"version", "config"}
        or isinstance(value.get("version"), bool)
        or not isinstance(value.get("version"), int)
        or value["version"] < 1
        or not isinstance(value.get("config"), dict)
    ):
        raise HTTPConfigurationProviderError("Remote configuration response has an invalid shape.")
    return value["version"], value["config"]


def _encode_config(value: object) -> bytes:
    """Encode the validated response object as bounded canonical UTF-8 JSON."""
    try:
        _validate_tree(value, depth=0, budget=[0])
        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        encoded = payload.encode("utf-8", errors="strict")
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise HTTPConfigurationProviderError("Remote configuration values are invalid.") from None
    if not 1 <= len(encoded) <= MAX_REMOTE_CONFIG_BYTES:
        raise HTTPConfigurationProviderError("Remote configuration values exceed their size limit.")
    return encoded


def _validate_tree(value: object, *, depth: int, budget: list[int]) -> None:
    """Bound the decoded tree before recursive serialization, matching the capability contract."""
    budget[0] += 1
    if budget[0] > 100_000:
        raise ValueError
    if isinstance(value, dict):
        if depth >= 64:
            raise ValueError
        for key, item in value.items():
            if (
                not isinstance(key, str)
                or not 1 <= len(key) <= 255
                or any(ord(character) < 32 or ord(character) == 127 for character in key)
            ):
                raise ValueError
            _validate_tree(item, depth=depth + 1, budget=budget)
    elif isinstance(value, list):
        if depth >= 64:
            raise ValueError
        for item in value:
            _validate_tree(item, depth=depth + 1, budget=budget)
    elif (
        value is None
        or isinstance(value, (bool, str, int))
        or (isinstance(value, float) and math.isfinite(value))
    ):
        return
    else:
        raise ValueError


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Reject duplicate JSON keys instead of accepting ambiguous remote values."""
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _reject_constant(_value: str) -> object:
    """Reject non-standard NaN and Infinity values."""
    raise ValueError


__all__ = ["HTTPConfigurationProviderError", "HTTPRemoteConfigurationProvider"]
