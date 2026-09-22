"""Runtime configuration, all overridable via environment variables."""
from __future__ import annotations

import os


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


class Settings:
    # persistence
    state_file: str = os.environ.get("GATEWAY_STATE_FILE", "gateway_state.json")

    # default credential lifetime (seconds); 0 means non-expiring
    default_credential_ttl: int = _int_env("GATEWAY_CREDENTIAL_TTL", 3600)

    # telemetry quota: max accepted messages per device per window
    telemetry_quota: int = _int_env("GATEWAY_TELEMETRY_QUOTA", 1000)
    telemetry_quota_window: float = _float_env("GATEWAY_TELEMETRY_QUOTA_WINDOW", 60.0)

    # firmware upgrade orchestration
    default_batch_size: int = _int_env("GATEWAY_BATCH_SIZE", 5)
    default_max_retries: int = _int_env("GATEWAY_MAX_RETRIES", 2)
    upgrade_dispatch_delay: float = _float_env("GATEWAY_UPGRADE_DELAY", 0.01)

    # simulator
    simulator_fail_rate: float = _float_env("GATEWAY_SIM_FAIL_RATE", 0.2)
