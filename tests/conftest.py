"""Shared fixtures: fully local stack backed by a temp JSON file."""
from __future__ import annotations

import asyncio
import logging
import tempfile

import pytest

from gateway.app import ServiceRegistry, create_app
from gateway.config import Settings


def _make_settings(**overrides):
    s = Settings()
    for k, v in overrides.items():
        setattr(s, k, v)
    return s


@pytest.fixture()
def tmp_state_file(tmp_path):
    return str(tmp_path / "gateway_state.json")


@pytest.fixture()
def settings(tmp_state_file):
    return _make_settings(
        state_file=tmp_state_file,
        default_credential_ttl=0,
        telemetry_quota=1000,
        telemetry_quota_window=60.0,
        default_batch_size=5,
        default_max_retries=2,
        upgrade_dispatch_delay=0.0,
        simulator_fail_rate=0.0,
    )


@pytest.fixture()
def registry(settings):
    reg = ServiceRegistry(settings)
    reg._wire()
    return reg


@pytest.fixture()
def client(registry):
    from fastapi.testclient import TestClient

    application = create_app(registry)
    with TestClient(application) as c:
        yield c


log = logging.getLogger("tests")


@pytest.fixture()
def registered_device(registry):
    """Return an async helper that registers a device and its credential."""

    async def _make(device_id: str = "dev1", **kwargs):
        from gateway.models import RegisterDeviceRequest

        req = RegisterDeviceRequest(device_id=device_id, **kwargs)
        view, secret = await registry.device_service.register(req, now=1000.0)
        log.info(
            "registered device_id=%s credential_id=%s ttl_expires=%s",
            device_id,
            view.credentials[0].credential_id,
            view.credentials[0].expires_at,
        )
        return view, secret

    return _make
