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
    STATE_UNAVAILABLE = "state_unavailable"             # durable state unreadable at startup
    STATE_PERSIST_FAILED = "state_persist_failed"       # durable snapshot could not be written


class GatewayError(Exception):
    """Domain error with a machine-distinguishable code."""

    def __init__(self, code: ErrorCode, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class BackupRejection(enum.Enum):
    """Machine-distinguishable reasons a backup package is refused."""
    TRUNCATED = "backup_truncated"                 # archive cut short / unreadable
    INTEGRITY_FAILED = "backup_integrity_failed"   # checksum mismatch / tampered
    FORMAT_UNSUPPORTED = "backup_format_unsupported"
    MISSING_SECTION = "backup_missing_section"     # required info absent
    PAYLOAD_INVALID = "backup_payload_invalid"     # present but unparseable
    NONEMPTY_TARGET = "backup_nonempty_target"     # restore only into fresh state


class BackupError(Exception):
    """Raised when a backup package cannot be produced or safely imported."""

    def __init__(self, code: BackupRejection, message: str):
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
