"""Error taxonomy for the IoT device gateway.

Every failure that crosses the API boundary maps to a ``GatewayError``
subclass so that callers can distinguish *why* a request was rejected
(for example: identifier conflict vs. expired credential vs. quota).
"""
from __future__ import annotations


class GatewayError(Exception):
    """Base class for all expected gateway errors."""

    error_code: str = "gateway_error"
    http_status: int = 400

    def __init__(self, message: str = "", *, details: dict | None = None) -> None:
        super().__init__(message or self.error_code)
        self.message = message or self.error_code
        self.details = details or {}


class DeviceConflictError(GatewayError):
    """Device identifier is already registered to a different registration."""

    error_code = "device_conflict"
    http_status = 409


class DeviceNotFoundError(GatewayError):
    """Referenced device does not exist."""

    error_code = "device_not_found"
    http_status = 404


class CredentialInvalidError(GatewayError):
    """Credential is malformed / does not match the device."""

    error_code = "credential_invalid"
    http_status = 401


class CredentialExpiredError(GatewayError):
    """Credential exists but its validity window has passed."""

    error_code = "credential_expired"
    http_status = 401


class CredentialRevokedError(GatewayError):
    """Credential has been explicitly revoked."""

    error_code = "credential_revoked"
    http_status = 401


class DeviceDisabledError(GatewayError):
    """Device is registered but has been disabled."""

    error_code = "device_disabled"
    http_status = 403


class ValidationFailureError(GatewayError):
    """Payload failed schema / value validation and must not be dropped."""

    error_code = "validation_failure"
    http_status = 422


class QuotaExceededError(GatewayError):
    """Message rate / volume quota exhausted."""

    error_code = "quota_exceeded"
    http_status = 429


class JobConflictError(GatewayError):
    """Operation conflicts with the current lifecycle state of a job/device."""

    error_code = "job_conflict"
    http_status = 409


class JobNotFoundError(GatewayError):
    """Upgrade job does not exist."""

    error_code = "job_not_found"
    http_status = 404
