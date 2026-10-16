from __future__ import annotations

import asyncio
import json

import httpx
import pytest
from orbit_config_server import RemoteConfigurationProvider
from pydantic import BaseModel

from orbit_config_server_http import (
    HTTPConfigurationProviderError,
    HTTPRemoteConfigurationProvider,
)


@pytest.fixture
def anyio_backend() -> str:
    """Keep the test matrix on the asyncio backend used by HTTPX's async client."""
    return "asyncio"


class _Settings(BaseModel):
    enabled: bool


def provider(
    handler,
    *,
    token: str | None = None,
    poll_interval: float = 0.1,
) -> HTTPRemoteConfigurationProvider:
    return HTTPRemoteConfigurationProvider(
        "https://config.example.test/base",
        "payments",
        bearer_token=token,
        poll_interval=poll_interval,
        transport=httpx.MockTransport(handler),
    )


@pytest.mark.anyio
async def test_fetch_uses_contract_and_encodes_section_safely() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            json={"version": 12, "config": {"enabled": True}},
        )

    async with provider(handler, token="sensitive-token") as current:
        result = await current.fetch()
    assert isinstance(current, RemoteConfigurationProvider)
    assert result.version == 12
    assert result.validate_as(_Settings).enabled is True
    assert seen[0].url.path == "/base/v1/config/payments"
    assert seen[0].headers["authorization"] == "Bearer sensitive-token"
    assert "sensitive-token" not in repr(current)


@pytest.mark.anyio
async def test_watch_yields_only_new_complete_snapshots() -> None:
    versions = iter((1, 1, 2))

    def handler(_request: httpx.Request) -> httpx.Response:
        version = next(versions)
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            json={"version": version, "config": {"enabled": version == 2}},
        )

    current = provider(handler, poll_interval=0.1)
    stream = current.watch(after_version=0)
    try:
        assert (await anext(stream)).version == 1
        assert (await anext(stream)).version == 2
    finally:
        await stream.aclose()
        await current.aclose()


@pytest.mark.anyio
async def test_watch_rejects_a_remote_version_older_than_persisted_cursor() -> None:
    current = provider(
        lambda _: httpx.Response(
            200,
            headers={"content-type": "application/json"},
            json={"version": 2, "config": {"enabled": True}},
        )
    )
    stream = current.watch(after_version=3)
    try:
        with pytest.raises(HTTPConfigurationProviderError, match="older than the requested cursor"):
            await anext(stream)
    finally:
        await stream.aclose()
        await current.aclose()


@pytest.mark.anyio
async def test_rejects_duplicate_keys_and_non_json_media_types() -> None:
    duplicate = provider(
        lambda _: httpx.Response(
            200,
            headers={"content-type": "application/json"},
            content=b'{"version":1,"config":{"x":1,"x":2}}',
        )
    )
    try:
        with pytest.raises(HTTPConfigurationProviderError, match="invalid JSON"):
            await duplicate.fetch()
    finally:
        await duplicate.aclose()

    media_type = provider(lambda _: httpx.Response(200, text="secret body"))
    try:
        with pytest.raises(HTTPConfigurationProviderError, match="media type") as error:
            await media_type.fetch()
        assert "secret body" not in str(error.value)
    finally:
        await media_type.aclose()


@pytest.mark.anyio
async def test_response_body_limit_is_enforced() -> None:
    too_large = b" " * (1_048_576 + 16_385)
    current = provider(
        lambda _: httpx.Response(
            200,
            headers={"content-type": "application/json"},
            content=too_large,
        )
    )
    try:
        with pytest.raises(HTTPConfigurationProviderError, match="size limit"):
            await current.fetch()
    finally:
        await current.aclose()


@pytest.mark.anyio
async def test_nested_and_non_finite_values_fail_before_recursive_encoding() -> None:
    nested: object = True
    for _ in range(70):
        nested = [nested]
    body = json.dumps({"version": 1, "config": {"nested": nested}}, separators=(",", ":"))
    current = provider(
        lambda _: httpx.Response(
            200,
            headers={"content-type": "application/json"},
            content=body,
        )
    )
    try:
        with pytest.raises(HTTPConfigurationProviderError, match="values are invalid"):
            await current.fetch()
    finally:
        await current.aclose()

    current = provider(
        lambda _: httpx.Response(
            200,
            headers={"content-type": "application/json"},
            content=b'{"version":1,"config":{"ratio":NaN}}',
        )
    )
    try:
        with pytest.raises(HTTPConfigurationProviderError, match="invalid JSON"):
            await current.fetch()
    finally:
        await current.aclose()


@pytest.mark.anyio
async def test_rejects_version_rollback_and_reuse_with_different_values() -> None:
    responses = iter(
        (
            {"version": 2, "config": {"enabled": True}},
            {"version": 2, "config": {"enabled": False}},
        )
    )
    current = provider(
        lambda _: httpx.Response(
            200,
            headers={"content-type": "application/json"},
            json=next(responses),
        )
    )
    try:
        await current.fetch()
        with pytest.raises(HTTPConfigurationProviderError, match="reused a version"):
            await current.fetch()
    finally:
        await current.aclose()


@pytest.mark.anyio
async def test_status_and_transport_errors_are_sanitized() -> None:
    current = provider(
        lambda _: httpx.Response(
            403,
            headers={"content-type": "application/json"},
            json={"error": "sensitive internal detail"},
        ),
        token="sensitive-token",
    )
    try:
        with pytest.raises(HTTPConfigurationProviderError) as error:
            await current.fetch()
        assert error.value.status_code == 403
        assert "sensitive" not in str(error.value)
        assert "sensitive-token" not in str(error.value)
    finally:
        await current.aclose()

    failed = provider(lambda _: httpx.Response(503, text="upstream secret"))
    try:
        with pytest.raises(HTTPConfigurationProviderError) as error:
            await failed.fetch()
        assert error.value.status_code == 503
        assert "upstream secret" not in str(error.value)
    finally:
        await failed.aclose()

    timeout = provider(
        lambda _: (_ for _ in ()).throw(httpx.ReadTimeout("sensitive transport details"))
    )
    try:
        with pytest.raises(HTTPConfigurationProviderError, match="timed out") as error:
            await timeout.fetch()
        assert "sensitive transport details" not in str(error.value)
    finally:
        await timeout.aclose()


@pytest.mark.anyio
async def test_cancelled_request_propagates_and_client_close_is_idempotent() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def handler(_request: httpx.Request) -> httpx.Response:
        entered.set()
        await release.wait()
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            json={"version": 1, "config": {"enabled": True}},
        )

    current = provider(handler)
    request = asyncio.create_task(current.fetch())
    await entered.wait()
    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request
    await current.aclose()
    await current.aclose()
    with pytest.raises(HTTPConfigurationProviderError, match="closed"):
        await current.fetch()


@pytest.mark.parametrize(
    ("base_url", "options"),
    [
        ("http://config.example.test", {}),
        ("https://user:pass@config.example.test", {}),
        ("https://config.example.test?token=unsafe", {}),
    ],
)
def test_rejects_unsafe_or_invalid_endpoint_configuration(base_url, options) -> None:
    with pytest.raises(ValueError):
        HTTPRemoteConfigurationProvider(base_url, "payments", **options)


@pytest.mark.anyio
async def test_local_http_is_available_only_with_explicit_opt_in() -> None:
    current = HTTPRemoteConfigurationProvider(
        "http://127.0.0.1:8000",
        "payments",
        allow_insecure_http=True,
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={"content-type": "application/json"},
                json={"version": 1, "config": {"enabled": True}},
            )
        ),
    )
    try:
        assert (await current.fetch()).version == 1
    finally:
        await current.aclose()


@pytest.mark.anyio
async def test_validates_poll_version_and_bounded_interval() -> None:
    current = provider(lambda _: httpx.Response(200), poll_interval=300)
    stream = current.watch(after_version=True)
    with pytest.raises(ValueError, match="after_version"):
        await anext(stream)
    stream = current.watch(after_version=-1)
    with pytest.raises(ValueError, match="after_version"):
        await anext(stream)
    await current.aclose()
    with pytest.raises(ValueError, match="poll_interval"):
        HTTPRemoteConfigurationProvider("https://config.example.test", "payments", poll_interval=0)
    with pytest.raises(ValueError, match="poll_jitter"):
        HTTPRemoteConfigurationProvider("https://config.example.test", "payments", poll_jitter=1)
    with pytest.raises(ValueError, match="bearer_token"):
        HTTPRemoteConfigurationProvider(
            "https://config.example.test", "payments", bearer_token="non-ascii-🔐"
        )
