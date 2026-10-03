"""FastAPI wiring for the IoT device gateway.

All state is in-memory (optionally JSON-persisted for FOTA campaigns); no
external broker, real devices, or real credentials are involved.
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

from app.backup import BackupManager
from app.durable import DurableState
from app.fota import FirmwareOrchestrator
from app.models import BackupError, ErrorCode, GatewayError
from app.registry import DeviceRegistry
from app.simulator import SimulatedFlasher
from app.telemetry import TelemetryStore

# --- configuration (env-overridable) -------------------------------------
CREDENTIAL_TTL_SECONDS = float(os.getenv("GATEWAY_CREDENTIAL_TTL_SECONDS", "3600"))
TELEMETRY_QUOTA = int(os.getenv("GATEWAY_TELEMETRY_QUOTA", "1000"))
MAX_CLOCK_SKEW_SECONDS = float(os.getenv("GATEWAY_MAX_CLOCK_SKEW_SECONDS", "300"))
FOTA_MAX_RETRIES = int(os.getenv("GATEWAY_FOTA_MAX_RETRIES", "2"))
FOTA_STATE_PATH = os.getenv("GATEWAY_FOTA_STATE_PATH") or None
# Full-gateway durable snapshot: makes backup restore survive restarts and
# redeployments (devices, credentials, shadows, dedup, rejects, campaigns,
# installed firmware). The FOTA-only file above is kept for compatibility.
STATE_PATH = os.getenv("GATEWAY_STATE_PATH") or None
STATE_FLUSH_INTERVAL = float(os.getenv("GATEWAY_STATE_FLUSH_INTERVAL_SECONDS",
                                      "0.1"))

# Registry/credential and per-device campaign steps are rare and crash-
# critical -> synchronous flush; high-rate telemetry -> coalesced flush.
_durable = DurableState(backups=None, path=STATE_PATH,
                        debounce_seconds=STATE_FLUSH_INTERVAL)  # type: ignore[arg-type]
_urgent = _durable.flush_urgent
_soon = _durable.flush_soon

registry = DeviceRegistry(credential_ttl_seconds=CREDENTIAL_TTL_SECONDS,
                          on_change=_urgent)
telemetry = TelemetryStore(per_device_quota=TELEMETRY_QUOTA,
                           max_clock_skew_seconds=MAX_CLOCK_SKEW_SECONDS,
                           on_change=_soon)
flasher = SimulatedFlasher()
orchestrator = FirmwareOrchestrator(registry, flasher,
                                    max_retries=FOTA_MAX_RETRIES,
                                    state_path=FOTA_STATE_PATH,
                                    on_change=_urgent)
backups = BackupManager(registry, telemetry, orchestrator, flasher)
_durable._backups = backups
backups._durable = _durable

# Recover the durable whole on boot. If the snapshot (and its predecessor)
# is unusable the gateway stays up but faulted: business calls get a clear
# 503 and an admin backup restore clears the fault. Nothing silently
# degrades to an empty gateway.
_startup_summary = _durable.load_at_startup()


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    # SIGTERM/SIGINT-driven graceful shutdown: force coalesced telemetry out
    _durable.shutdown()


app = FastAPI(title="IoT Device Gateway", lifespan=lifespan)

_ERROR_STATUS = {
    ErrorCode.DEVICE_NOT_FOUND: 404,
    ErrorCode.TASK_NOT_FOUND: 404,
    ErrorCode.DEVICE_ID_CONFLICT: 409,
    ErrorCode.TASK_STATE_CONFLICT: 409,
    ErrorCode.DEVICE_BUSY: 409,
    ErrorCode.DEVICE_DISABLED: 403,
    ErrorCode.DEVICE_DEREGISTERED: 403,
    ErrorCode.CREDENTIAL_INVALID: 401,
    ErrorCode.CREDENTIAL_EXPIRED: 401,
    ErrorCode.CREDENTIAL_REVOKED: 401,
    ErrorCode.QUOTA_EXCEEDED: 429,
    ErrorCode.VALIDATION_FAILED: 422,
    ErrorCode.STATE_UNAVAILABLE: 503,
    ErrorCode.STATE_PERSIST_FAILED: 500,
}

_BACKUP_ERROR_STATUS = {
    # bad package itself: unreadable / checksum / version / sections / payload
    "backup_truncated": 422,
    "backup_integrity_failed": 422,
    "backup_format_unsupported": 422,
    "backup_missing_section": 422,
    "backup_payload_invalid": 422,
    # package is fine but cannot be applied to this gateway
    "backup_nonempty_target": 409,
}


@app.exception_handler(GatewayError)
async def gateway_error_handler(request, exc: GatewayError):
    return JSONResponse(
        status_code=_ERROR_STATUS.get(exc.code, 400),
        content={"detail": {"code": exc.code.value, "message": exc.message}})


@app.exception_handler(BackupError)
async def backup_error_handler(request: Request, exc: BackupError):
    return JSONResponse(
        status_code=_BACKUP_ERROR_STATUS.get(exc.code.value, 400),
        content={"detail": {"code": exc.code.value, "message": exc.message}})


# --- request bodies --------------------------------------------------------
class RegisterBody(BaseModel):
    device_id: str
    secret: str


class RotateBody(BaseModel):
    secret: str


class TelemetryBody(BaseModel):
    message_id: str
    seq: int = Field(ge=0)
    device_ts: float = Field(ge=0)
    payload: dict


class CampaignBody(BaseModel):
    campaign_id: str
    target_version: str
    device_ids: list[str]
    batch_size: int = Field(default=10, ge=1)


def _auth(device_id: str, secret: str | None):
    _durable.require_healthy()
    if secret is None:
        raise HTTPException(status_code=401, detail={
            "code": ErrorCode.CREDENTIAL_INVALID.value,
            "message": "missing X-Device-Secret header"})
    return registry.authenticate(device_id, secret)


# --- device lifecycle ------------------------------------------------------
@app.post("/devices/register")
def register(body: RegisterBody):
    _durable.require_healthy()
    device = registry.register(body.device_id, body.secret)
    return {"device_id": device.device_id, "status": device.status.value,
            "registered_at": device.registered_at}


@app.post("/devices/{device_id}/disable")
def disable(device_id: str):
    _durable.require_healthy()
    return {"status": registry.disable(device_id).status.value}


@app.post("/devices/{device_id}/enable")
def enable(device_id: str):
    _durable.require_healthy()
    return {"status": registry.enable(device_id).status.value}


@app.post("/devices/{device_id}/deregister")
def deregister(device_id: str):
    _durable.require_healthy()
    return {"status": registry.deregister(device_id).status.value}


@app.post("/devices/{device_id}/revoke")
def revoke(device_id: str):
    _durable.require_healthy()
    registry.revoke_credential(device_id)
    return {"credential": "revoked"}


@app.post("/devices/{device_id}/rotate")
def rotate(device_id: str, body: RotateBody):
    _durable.require_healthy()
    registry.rotate_credential(device_id, body.secret)
    return {"credential": "rotated"}


# --- telemetry & shadow ----------------------------------------------------
@app.post("/devices/{device_id}/telemetry")
def ingest(device_id: str, body: TelemetryBody,
           x_device_secret: str | None = Header(default=None)):
    _auth(device_id, x_device_secret)
    result = telemetry.ingest(device_id, body.message_id, body.seq,
                              body.device_ts, body.payload)
    if not result.accepted:
        code = ErrorCode(result.code)
        raise HTTPException(status_code=_ERROR_STATUS.get(code, 400),
                            detail={"code": code.value,
                                    "message": "telemetry rejected"})
    return {"accepted": True, "code": result.code,
            "decisions": [d.__dict__ for d in result.decisions]}


@app.get("/devices/{device_id}/shadow")
def shadow(device_id: str):
    _durable.require_healthy()
    registry.get(device_id)
    return {"device_id": device_id, "shadow": telemetry.shadow(device_id),
            "field_versions": telemetry.field_versions(device_id)}


@app.get("/devices/{device_id}/telemetry/rejected")
def rejected(device_id: str):
    _durable.require_healthy()
    return {"rejected": [r.code for r in telemetry.rejected(device_id)]}


# --- firmware campaigns ----------------------------------------------------
@app.post("/campaigns")
def create_campaign(body: CampaignBody):
    _durable.require_healthy()
    return orchestrator.create_campaign(body.campaign_id, body.target_version,
                                        body.device_ids, body.batch_size)


@app.post("/campaigns/{campaign_id}/run")
def run_campaign(campaign_id: str):
    _durable.require_healthy()
    return orchestrator.run_until_blocked(campaign_id)


@app.post("/campaigns/{campaign_id}/pause")
def pause_campaign(campaign_id: str):
    _durable.require_healthy()
    return orchestrator.pause(campaign_id)


@app.post("/campaigns/{campaign_id}/resume")
def resume_campaign(campaign_id: str):
    _durable.require_healthy()
    return orchestrator.resume(campaign_id)


@app.post("/campaigns/{campaign_id}/cancel")
def cancel_campaign(campaign_id: str):
    _durable.require_healthy()
    return orchestrator.cancel(campaign_id)


@app.get("/campaigns/{campaign_id}")
def campaign_status(campaign_id: str):
    _durable.require_healthy()
    return orchestrator.status(campaign_id)


@app.get("/")
def read_root():
    return {"service": "iot-device-gateway", "docs": "/docs"}


@app.get("/health")
def health():
    """Durable-state health: observable proof that a restart recovered a
    complete snapshot (or that the store is faulted and needs an import)."""
    info = _durable.status()
    info["startup_restore"] = _startup_summary
    return info


# --- backup / restore (migration & disaster-recovery) ---------------------
@app.post("/admin/backup/export")
def export_backup():
    """Online full backup; the service keeps running during the export.

    Refused while the durable store is faulted, since exporting a broken
    half-state would just propagate the damage.
    """
    _durable.require_healthy()
    raw = backups.export_bytes()
    return Response(
        content=raw,
        media_type="application/zip",
        headers={"Content-Disposition":
                 'attachment; filename="gateway-backup.zip"'})


@app.post("/admin/backup/inspect")
async def inspect_backup(request: Request):
    """Validate an uploaded package without changing any live state."""
    raw = await request.body()
    return backups.inspect_package(raw)


@app.post("/admin/backup/restore")
async def restore_backup(request: Request,
                         x_allow_overwrite: str | None = Header(default=None)):
    """Restore onto a fresh gateway.

    Refuses (409 backup_nonempty_target) when the target already holds state,
    unless the X-Allow-Overwrite: true header is present. Any validation
    failure leaves the current state untouched. Also the documented escape
    hatch when durable state failed to load at startup: a successful import
    clears the faulted flag.
    """
    raw = await request.body()
    allow = (x_allow_overwrite or "").strip().lower() in ("1", "true", "yes")
    summary = backups.restore_bytes(raw, allow_overwrite=allow)
    return summary
