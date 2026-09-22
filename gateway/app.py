"""FastAPI application wiring.

Endpoints are grouped under /devices, /telemetry and /upgrades.
This module exposes ``create_app`` and a process-wide service registry
that also loads persisted state on startup.
"""
from __future__ import annotations

import contextlib

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .config import Settings
from .errors import GatewayError
from .simulator import DeviceSimulator
from .store import DataStore


class ServiceRegistry:
    """Process-wide singletons, swappable by tests."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings
        self.store = DataStore(settings.state_file)
        self.simulator = DeviceSimulator(
            fail_rate=settings.simulator_fail_rate,
            delay=settings.upgrade_dispatch_delay,
        )
        self.device_service = None  # set in _wire()
        self.telemetry_service = None
        self.firmware_service = None

    def _wire(self) -> None:
        # Imported lazily to keep the import graph simple.
        from .devices import DeviceService
        from .firmware import FirmwareService
        from .telemetry import TelemetryService

        s = self.settings
        self.device_service = DeviceService(
            self.store, default_ttl=s.default_credential_ttl
        )
        self.telemetry_service = TelemetryService(
            self.store,
            self.device_service,
            quota=s.telemetry_quota,
            quota_window=s.telemetry_quota_window,
        )
        self.firmware_service = FirmwareService(
            self.store,
            self.device_service,
            self.simulator,
            batch_size=s.default_batch_size,
            max_retries=s.default_max_retries,
            dispatch_delay=s.upgrade_dispatch_delay,
        )


def create_app(registry: ServiceRegistry | None = None) -> FastAPI:
    reg = registry or ServiceRegistry(Settings())
    reg._wire()

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        await reg.store.load()
        await reg.firmware_service.resume_pending_jobs()
        yield

    application = FastAPI(title="IoT Device Gateway", lifespan=lifespan)
    application.state.registry = reg

    @application.exception_handler(GatewayError)
    async def _gateway_error_handler(_: Request, exc: GatewayError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.http_status,
            content={
                "error": exc.error_code,
                "message": exc.message,
                "details": exc.details,
            },
        )

    @application.get("/")
    async def health() -> dict:
        return {"status": "ok", "service": "iot-device-gateway"}

    from .routers import devices, telemetry, upgrades

    application.include_router(devices.router)
    application.include_router(telemetry.router)
    application.include_router(upgrades.router)

    return application


app = create_app()
