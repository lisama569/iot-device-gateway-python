"""Shared types: enums, error codes, data records."""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class DeviceStatus(enum.Enum):
    ACTIVE = "active"
    DISABLED = "disabled"
    DEREGISTERED = "deregistered"


class ErrorCode(enum.Enum):
    DEVICE_NOT_FOUND = "device_not_found"
    DEVICE_ID_CONFLICT = "device_id_conflict"
    DEVICE_DISABLED = "device_disabled"
    DEVICE_DEREGISTERED = "device_deregistered"
    CREDENTIAL_INVALID = "credential_invalid"
    CREDENTIAL_EXPIRED = "credential_expired"
    CREDENTIAL_REVOKED = "credential_revoked"
    QUOTA_EXCEEDED = "quota_exceeded"
    VALIDATION_FAILED = "validation_failed"
    DUPLICATE_MESSAGE = "duplicate_message"
    STALE_MESSAGE = "stale_message"
    TASK_NOT_FOUND = "task_not_found"
    TASK_STATE_CONFLICT = "task_state_conflict"
    DEVICE_BUSY = "device_busy"
    BACKUP_CORRUPTED = "backup_corrupted"
    BACKUP_INTEGRITY_FAILED = "backup_integrity_failed"
    BACKUP_VERSION_UNSUPPORTED = "backup_version_unsupported"
    BACKUP_INCOMPLETE = "backup_incomplete"


class GatewayError(Exception):
    """Domain error with a machine-distinguishable code."""

    def __init__(self, code: ErrorCode, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class CampaignStatus(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    CANCELLED = "cancelled"
    COMPLETED = "completed"
    FAILED = "failed"


TERMINAL_STATES = {CampaignStatus.CANCELLED, CampaignStatus.COMPLETED, CampaignStatus.FAILED}


class DeviceUpdateState(enum.Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"  # cancelled before dispatch
