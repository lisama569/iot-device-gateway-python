"""FastAPI wiring for the IoT device gateway.

All state is in-memory (optionally JSON-persisted for FOTA campaigns); no
external broker, real devices, or real credentials are involved.
"""
from __future__ import annotations

import os

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

from app.backup import BackupCoordinator
from app.fota import FirmwareOrchestrator
from app.models import ErrorCode, GatewayError
from app.registry import DeviceRegistry
from app.simulator import SimulatedFlasher
from app.telemetry import TelemetryStore

# --- configuration (env-overridable) -------------------------------------
CREDENTIAL_TTL_SECONDS = float(os.getenv("GATEWAY_CREDENTIAL_TTL_SECONDS", "3600"))
TELEMETRY_QUOTA = int(os.getenv("GATEWAY_TELEMETRY_QUOTA", "1000"))
MAX_CLOCK_SKEW_SECONDS = float(os.getenv("GATEWAY_MAX_CLOCK_SKEW_SECONDS", "300"))
FOTA_MAX_RETRIES = int(os.getenv("GATEWAY_FOTA_MAX_RETRIES", "2"))
# State directory: when set, registry / telemetry / campaigns are each
# persisted as an atomic JSON file and auto-recovered on the next start, so a
# restored deployment also survives ordinary process restarts.
STATE_DIR = os.getenv("GATEWAY_STATE_DIR") or None
if STATE_DIR:
    os.makedirs(STATE_DIR, exist_ok=True)
FOTA_STATE_PATH = os.getenv("GATEWAY_FOTA_STATE_PATH") or (
    os.path.join(STATE_DIR, "fota.json") if STATE_DIR else None)
REGISTRY_STATE_PATH = (os.path.join(STATE_DIR, "registry.json")
                       if STATE_DIR else None)
TELEMETRY_STATE_PATH = (os.path.join(STATE_DIR, "telemetry.json")
                        if STATE_DIR else None)
FLASHER_STATE_PATH = (os.path.join(STATE_DIR, "devices.json")
                      if STATE_DIR else None)

registry = DeviceRegistry(credential_ttl_seconds=CREDENTIAL_TTL_SECONDS,
                          state_path=REGISTRY_STATE_PATH)
telemetry = TelemetryStore(per_device_quota=TELEMETRY_QUOTA,
                           max_clock_skew_seconds=MAX_CLOCK_SKEW_SECONDS,
                           state_path=TELEMETRY_STATE_PATH)
flasher = SimulatedFlasher(state_path=FLASHER_STATE_PATH)
orchestrator = FirmwareOrchestrator(registry, flasher,
                                    max_retries=FOTA_MAX_RETRIES,
                                    state_path=FOTA_STATE_PATH)
backups = BackupCoordinator(registry, telemetry, orchestrator, flasher)

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
    ErrorCode.BACKUP_CORRUPTED: 400,
    ErrorCode.BACKUP_INTEGRITY_FAILED: 422,
    ErrorCode.BACKUP_VERSION_UNSUPPORTED: 422,
    ErrorCode.BACKUP_INCOMPLETE: 422,
}


@app.exception_handler(GatewayError)
async def gateway_error_handler(request, exc: GatewayError):
    return JSONResponse(
        status_code=_ERROR_STATUS.get(exc.code, 400),
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


# --- backup & restore ------------------------------------------------------
class RestorePathBody(BaseModel):
    path: str


@app.post("/admin/backup")
def export_backup(body: RestorePathBody | None = None):
    """Online full-state export; service keeps running.

    With a JSON body {"path": "..."} the backup is atomically written there;
    without a body it is streamed back as the response (Content-Disposition).
    """
    if body is not None and body.path:
        result = backups.export_to_file(body.path)
        return {"exported": True, **result}
    raw = backups.export_bytes()
    return Response(
        content=raw, media_type="application/json",
        headers={"Content-Disposition":
                 'attachment; filename="gateway-backup.json"'})


@app.post("/admin/restore")
async def restore_backup(request: Request):
    """Validate then apply an uploaded backup; failed imports change nothing."""
    raw = await request.body()
    return {"restored": True, **backups.restore_bytes(raw)}


@app.post("/admin/restore-file")
def restore_backup_file(body: RestorePathBody):
    """Validate then apply a backup from a local path (migration on one box)."""
    return {"restored": True, **backups.restore_from_file(body.path)}


@app.get("/")
def read_root():
    return {"service": "iot-device-gateway", "docs": "/docs"}
