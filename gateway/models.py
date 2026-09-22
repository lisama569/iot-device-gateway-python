"""Pydantic models (request/response DTOs and enums) for the gateway."""
from __future__ import annotations

import time
from enum import Enum

from pydantic import BaseModel, Field


class DeviceStatus(str, Enum):
    ENABLED = "enabled"
    DISABLED = "disabled"
    DEREGISTERED = "deregistered"


class CredentialStatus(str, Enum):
    ACTIVE = "active"
    REVOKED = "revoked"
    EXPIRED = "expired"  # derived at check time when now > expires_at


class JobState(str, Enum):
    PENDING = "pending"        # created, not started
    IN_PROGRESS = "in_progress"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class DeviceUpgradeState(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


# ---- device / credential DTOs ----

class RegisterDeviceRequest(BaseModel):
    device_id: str = Field(min_length=1, max_length=128)
    name: str = ""
    metadata: dict = Field(default_factory=dict)
    credential_ttl_seconds: int | None = Field(default=None, ge=1)


class CredentialView(BaseModel):
    credential_id: str
    status: CredentialStatus
    expires_at: float | None = None
    issued_at: float = Field(default_factory=time.time)
    revoked_at: float | None = None


class DeviceView(BaseModel):
    device_id: str
    name: str
    status: DeviceStatus
    metadata: dict
    registration_seq: int
    created_at: float
    updated_at: float
    credentials: list[CredentialView] = Field(default_factory=list)


class RotateCredentialRequest(BaseModel):
    credential_ttl_seconds: int | None = Field(default=None, ge=1)
    revoke_previous: bool = True


class TelemetryMessage(BaseModel):
    device_id: str = Field(min_length=1, max_length=128)
    credential_id: str
    credential_secret: str = Field(min_length=1)
    version: int = Field(ge=1, description="monotonic per-device state version")
    timestamp: float = Field(description="device-side event timestamp (epoch seconds)")
    state: dict = Field(default_factory=dict)
    seq: int = Field(default=0, ge=0)
    nonce: str | None = None


class TelemetryResult(BaseModel):
    accepted: bool
    device_id: str
    current_version: int
    merged_state: dict
    decision: str
    reason: str


class ShadowView(BaseModel):
    device_id: str
    version: int
    state: dict
    updated_at: float
    last_source_version: int | None = None


# ---- firmware upgrade DTOs ----

class CreateJobRequest(BaseModel):
    job_id: str | None = None
    firmware_version: str = Field(min_length=1, max_length=128)
    device_ids: list[str] = Field(min_length=1)
    batch_size: int = Field(default=5, ge=1)
    max_retries: int = Field(default=2, ge=0, le=10)


class DeviceUpgradeView(BaseModel):
    device_id: str
    state: DeviceUpgradeState
    firmware_version: str
    attempts: int
    last_error: str | None = None
    updated_at: float


class JobView(BaseModel):
    job_id: str
    firmware_version: str
    state: JobState
    batch_size: int
    max_retries: int
    next_batch_index: int
    total_devices: int
    device_states: dict[str, DeviceUpgradeView]
    created_at: float
    updated_at: float
    finished_at: float | None = None
    terminal_reason: str | None = None


class JobControlRequest(BaseModel):
    reason: str = ""
