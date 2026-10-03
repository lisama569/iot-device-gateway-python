"""FastAPI wiring for the IoT device gateway.

State durability: when GATEWAY_STATE_PATH is set, the whole gateway state
(registry, telemetry, FOTA campaigns, installed firmware versions) is
checkpointed to that file after every mutation and reloaded on boot, so a
restored gateway keeps working across restarts/redeploys. Otherwise all
state is in-memory (FOTA campaigns may still be JSON-persisted separately
via GATEWAY_FOTA_STATE_PATH). No external broker, real devices, or real
credentials are involved.
"""
from __future__ import annotations

import logging
import os

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

from app.backup import BackupManager
from app.fota import FirmwareOrchestrator
from app.models import BackupError, ErrorCode, GatewayError
from app.persistence import StateStore
from app.registry import DeviceRegistry
from app.simulator import SimulatedFlasher
from app.telemetry import TelemetryStore

logger = logging.getLogger("iot-gateway")

# --- configuration (env-overridable) -------------------------------------
CREDENTIAL_TTL_SECONDS = float(os.getenv("GATEWAY_CREDENTIAL_TTL_SECONDS", "3600"))
TELEMETRY_QUOTA = int(os.getenv("GATEWAY_TELEMETRY_QUOTA", "1000"))
MAX_CLOCK_SKEW_SECONDS = float(os.getenv("GATEWAY_MAX_CLOCK_SKEW_SECONDS", "300"))
FOTA_MAX_RETRIES = int(os.getenv("GATEWAY_FOTA_MAX_RETRIES", "2"))
GATEWAY_STATE_PATH = os.getenv("GATEWAY_STATE_PATH") or None
# the unified state file supersedes the FOTA-only state file
FOTA_STATE_PATH = (None if GATEWAY_STATE_PATH
                   else os.getenv("GATEWAY_FOTA_STATE_PATH") or None)

registry = DeviceRegistry(credential_ttl_seconds=CREDENTIAL_TTL_SECONDS)
telemetry = TelemetryStore(per_device_quota=TELEMETRY_QUOTA,
                           max_clock_skew_seconds=MAX_CLOCK_SKEW_SECONDS)
flasher = SimulatedFlasher()
orchestrator = FirmwareOrchestrator(registry, flasher,
                                    max_retries=FOTA_MAX_RETRIES,
                                    state_path=FOTA_STATE_PATH)
backups = BackupManager(registry, telemetry, orchestrator, flasher)

state_store: StateStore | None = None
if GATEWAY_STATE_PATH:
    state_store = StateStore(backups, GATEWAY_STATE_PATH).attach()
    if state_store.load() == "error":
        logger.error("gateway state checkpoint rejected: %s (quarantined to "
                     "%s); starting empty, backup import still available",
                     state_store.status()["last_error"],
                     state_store.status()["quarantined_to"])

app = FastAPI(title="IoT Device Gateway")

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
    if secret is None:
        raise HTTPException(status_code=401, detail={
            "code": ErrorCode.CREDENTIAL_INVALID.value,
            "message": "missing X-Device-Secret header"})
    return registry.authenticate(device_id, secret)


# --- device lifecycle ------------------------------------------------------
@app.post("/devices/register")
def register(body: RegisterBody):
    device = registry.register(body.device_id, body.secret)
    return {"device_id": device.device_id, "status": device.status.value,
            "registered_at": device.registered_at}


@app.post("/devices/{device_id}/disable")
def disable(device_id: str):
    return {"status": registry.disable(device_id).status.value}


@app.post("/devices/{device_id}/enable")
def enable(device_id: str):
    return {"status": registry.enable(device_id).status.value}


@app.post("/devices/{device_id}/deregister")
def deregister(device_id: str):
    return {"status": registry.deregister(device_id).status.value}


@app.post("/devices/{device_id}/revoke")
def revoke(device_id: str):
    registry.revoke_credential(device_id)
    return {"credential": "revoked"}


@app.post("/devices/{device_id}/rotate")
def rotate(device_id: str, body: RotateBody):
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
    registry.get(device_id)
    return {"device_id": device_id, "shadow": telemetry.shadow(device_id),
            "field_versions": telemetry.field_versions(device_id)}


@app.get("/devices/{device_id}/telemetry/rejected")
def rejected(device_id: str):
    return {"rejected": [r.code for r in telemetry.rejected(device_id)]}


# --- firmware campaigns ----------------------------------------------------
@app.post("/campaigns")
def create_campaign(body: CampaignBody):
    return orchestrator.create_campaign(body.campaign_id, body.target_version,
                                        body.device_ids, body.batch_size)


@app.post("/campaigns/{campaign_id}/run")
def run_campaign(campaign_id: str):
    return orchestrator.run_until_blocked(campaign_id)


@app.post("/campaigns/{campaign_id}/pause")
def pause_campaign(campaign_id: str):
    return orchestrator.pause(campaign_id)


@app.post("/campaigns/{campaign_id}/resume")
def resume_campaign(campaign_id: str):
    return orchestrator.resume(campaign_id)


@app.post("/campaigns/{campaign_id}/cancel")
def cancel_campaign(campaign_id: str):
    return orchestrator.cancel(campaign_id)


@app.get("/campaigns/{campaign_id}")
def campaign_status(campaign_id: str):
    return orchestrator.status(campaign_id)


@app.get("/")
def read_root():
    return {"service": "iot-device-gateway", "docs": "/docs"}


# --- backup / restore (migration & disaster-recovery) ---------------------
@app.post("/admin/backup/export")
def export_backup():
    """Online full backup; the service keeps running during the export."""
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
    failure leaves the current state untouched.
    """
    raw = await request.body()
    allow = (x_allow_overwrite or "").strip().lower() in ("1", "true", "yes")
    return backups.restore_bytes(raw, allow_overwrite=allow)


@app.get("/admin/state")
def state_persistence_status():
    """Restart-persistence health: whether the boot checkpoint was loaded,
    or the explicit reason it was rejected (and where it was quarantined)."""
    if state_store is None:
        return {"enabled": False,
                "hint": "set GATEWAY_STATE_PATH to survive restarts"}
    return state_store.status()
