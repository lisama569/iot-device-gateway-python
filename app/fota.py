"""Firmware upgrade orchestration.

Deterministic rules:
- A campaign processes its target devices in batches of `batch_size`, in the
  order given at creation.
- Per-device failure is retried up to `max_retries` extra attempts; a device
  that still fails is marked FAILED and the campaign ends FAILED after the
  current batch finishes (undispatched devices -> SKIPPED). The outcome is
  therefore always explainable from the per-device states.
- Terminal state is unique: COMPLETED (all succeeded), FAILED (a device
  exhausted retries), CANCELLED (operator cancel). No transitions out of a
  terminal state.
- A device may not be targeted by two non-terminal campaigns (DEVICE_BUSY).
- If state_path is set, state is persisted after every mutation; recover()
  reloads it after a restart: RUNNING campaigns become PAUSED (never stuck)
  and interrupted IN_PROGRESS devices go back to PENDING. The flasher is
  expected to be idempotent (the bundled simulator is), so re-dispatch does
  not re-deliver firmware to a device that already has it.
"""
from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from typing import Callable

from .models import (CampaignStatus, DeviceStatus, DeviceUpdateState,
                     ErrorCode, GatewayError, TERMINAL_STATES)
from .registry import DeviceRegistry

Flasher = Callable[[str, str], None]


@dataclass
class Campaign:
    campaign_id: str
    target_version: str
    device_ids: list[str]
    batch_size: int
    status: CampaignStatus = CampaignStatus.PENDING
    device_states: dict[str, DeviceUpdateState] = field(default_factory=dict)
    attempts: dict[str, int] = field(default_factory=dict)
    current_batch: int = 0
    error: str | None = None
    updated_at: float = 0.0

    def batches(self) -> list[list[str]]:
        return [self.device_ids[i:i + self.batch_size]
                for i in range(0, len(self.device_ids), self.batch_size)]

    def to_dict(self) -> dict:
        return {
            "campaign_id": self.campaign_id,
            "target_version": self.target_version,
            "device_ids": list(self.device_ids),
            "batch_size": self.batch_size,
            "status": self.status.value,
            "device_states": {k: v.value for k, v in self.device_states.items()},
            "attempts": dict(self.attempts),
            "current_batch": self.current_batch,
            "error": self.error,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Campaign":
        return cls(
            campaign_id=data["campaign_id"],
            target_version=data["target_version"],
            device_ids=list(data["device_ids"]),
            batch_size=data["batch_size"],
            status=CampaignStatus(data["status"]),
            device_states={k: DeviceUpdateState(v)
                           for k, v in data["device_states"].items()},
            attempts={k: int(v) for k, v in data["attempts"].items()},
            current_batch=data["current_batch"],
            error=data.get("error"),
            updated_at=data.get("updated_at", 0.0),
        )


class FirmwareOrchestrator:
    def __init__(self, registry: DeviceRegistry, flasher: Flasher,
                 max_retries: int = 2, state_path: str | None = None,
                 clock=time.time):
        self._registry = registry
        self._flasher = flasher
        self._max_retries = max_retries
        self._state_path = state_path
        self._clock = clock
        self._campaigns: dict[str, Campaign] = {}
        self._lock = threading.RLock()
        if state_path:
            self.recover()

    # -- persistence -----------------------------------------------------
    def _persist(self) -> None:
        if not self._state_path:
            return
        data = [c.to_dict() for c in self._campaigns.values()]
        tmp = self._state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)
        import os
        os.replace(tmp, self._state_path)

    def recover(self) -> None:
        """Reload persisted state after restart; nothing left stuck."""
        import os
        if not self._state_path or not os.path.exists(self._state_path):
            return
        with open(self._state_path, encoding="utf-8") as fh:
            data = json.load(fh)
        with self._lock:
            self._campaigns = {c["campaign_id"]: Campaign.from_dict(c)
                               for c in data}
            for campaign in self._campaigns.values():
                if campaign.status is CampaignStatus.RUNNING:
                    # Was mid-dispatch when the process died: park it so an
                    # operator can resume explicitly; never leave it stuck.
                    campaign.status = CampaignStatus.PAUSED
                for dev, state in campaign.device_states.items():
                    if state is DeviceUpdateState.IN_PROGRESS:
                        campaign.device_states[dev] = DeviceUpdateState.PENDING
                campaign.updated_at = self._clock()
            self._persist()

    # -- lifecycle -------------------------------------------------------
    def create_campaign(self, campaign_id: str, target_version: str,
                        device_ids: list[str], batch_size: int = 10) -> dict:
        if not campaign_id or not target_version or not device_ids:
            raise GatewayError(ErrorCode.VALIDATION_FAILED,
                               "campaign_id, target_version and device_ids required")
        if batch_size < 1:
            raise GatewayError(ErrorCode.VALIDATION_FAILED,
                               "batch_size must be >= 1")
        with self._lock:
            existing = self._campaigns.get(campaign_id)
            if existing is not None:
                if (existing.target_version == target_version
                        and existing.device_ids == list(device_ids)
                        and existing.batch_size == batch_size):
                    return existing.to_dict()  # idempotent replay
                raise GatewayError(ErrorCode.TASK_STATE_CONFLICT,
                                   f"campaign {campaign_id!r} exists with "
                                   "different parameters")
            for dev in device_ids:
                device = self._registry.get(dev)  # raises DEVICE_NOT_FOUND
                if device.status is not DeviceStatus.ACTIVE:
                    raise GatewayError(ErrorCode.DEVICE_DISABLED,
                                       f"device {dev!r} is not active")
                self._check_device_free(dev)
            campaign = Campaign(
                campaign_id=campaign_id,
                target_version=target_version,
                device_ids=list(device_ids),
                batch_size=batch_size,
                device_states={d: DeviceUpdateState.PENDING for d in device_ids},
                attempts={d: 0 for d in device_ids},
                updated_at=self._clock(),
            )
            self._campaigns[campaign_id] = campaign
            self._persist()
            return campaign.to_dict()

    def _check_device_free(self, device_id: str) -> None:
        for other in self._campaigns.values():
            if other.status in TERMINAL_STATES:
                continue
            state = other.device_states.get(device_id)
            if state in (DeviceUpdateState.PENDING, DeviceUpdateState.IN_PROGRESS):
                raise GatewayError(
                    ErrorCode.DEVICE_BUSY,
                    f"device {device_id!r} already scheduled in campaign "
                    f"{other.campaign_id!r}")

    def _require(self, campaign_id: str) -> Campaign:
        campaign = self._campaigns.get(campaign_id)
        if campaign is None:
            raise GatewayError(ErrorCode.TASK_NOT_FOUND,
                               f"unknown campaign {campaign_id!r}")
        return campaign

    def _require_not_terminal(self, campaign: Campaign) -> None:
        if campaign.status in TERMINAL_STATES:
            raise GatewayError(ErrorCode.TASK_STATE_CONFLICT,
                               f"campaign {campaign.campaign_id!r} is already "
                               f"{campaign.status.value}")

    # -- execution -------------------------------------------------------
    def run_until_blocked(self, campaign_id: str) -> dict:
        """Dispatch batches until the campaign is paused, cancelled, or terminal."""
        with self._lock:
            campaign = self._require(campaign_id)
            if campaign.status is CampaignStatus.PENDING:
                campaign.status = CampaignStatus.RUNNING
                campaign.updated_at = self._clock()
                self._persist()
            if campaign.status is not CampaignStatus.RUNNING:
                if campaign.status in TERMINAL_STATES:
                    return campaign.to_dict()
                raise GatewayError(ErrorCode.TASK_STATE_CONFLICT,
                                   f"campaign is {campaign.status.value}; "
                                   "resume it first")
        while True:
            with self._lock:
                if campaign.status is not CampaignStatus.RUNNING:
                    return campaign.to_dict()
                batches = campaign.batches()
                if campaign.current_batch >= len(batches):
                    campaign.status = CampaignStatus.COMPLETED
                    campaign.updated_at = self._clock()
                    self._persist()
                    return campaign.to_dict()
                batch = batches[campaign.current_batch]
                for dev in batch:
                    if campaign.device_states[dev] is DeviceUpdateState.PENDING:
                        campaign.device_states[dev] = DeviceUpdateState.IN_PROGRESS
                campaign.updated_at = self._clock()
                self._persist()
            failed = self._run_batch(campaign, batch)
            with self._lock:
                campaign.current_batch += 1
                if failed:
                    campaign.status = CampaignStatus.FAILED
                    campaign.error = (f"device(s) {sorted(failed)} failed after "
                                      f"{self._max_retries + 1} attempts")
                    for dev in campaign.device_ids:
                        if campaign.device_states[dev] is DeviceUpdateState.PENDING:
                            campaign.device_states[dev] = DeviceUpdateState.SKIPPED
                campaign.updated_at = self._clock()
                self._persist()
                if failed:
                    return campaign.to_dict()

    def _run_batch(self, campaign: Campaign, batch: list[str]) -> set[str]:
        """Flash one batch (flasher called outside the lock). Returns failures."""
        failed: set[str] = set()
        for dev in batch:
            with self._lock:
                if campaign.device_states[dev] is not DeviceUpdateState.IN_PROGRESS:
                    continue
            ok = False
            for _ in range(self._max_retries + 1):
                with self._lock:
                    campaign.attempts[dev] += 1
                try:
                    self._flasher(dev, campaign.target_version)
                    ok = True
                    break
                except Exception:
                    continue
            with self._lock:
                if ok:
                    campaign.device_states[dev] = DeviceUpdateState.SUCCEEDED
                    device = self._registry.get(dev)
                    device.firmware_version = campaign.target_version
                else:
                    campaign.device_states[dev] = DeviceUpdateState.FAILED
                    failed.add(dev)
                campaign.updated_at = self._clock()
                self._persist()
        return failed

    # -- control ---------------------------------------------------------
    def pause(self, campaign_id: str) -> dict:
        with self._lock:
            campaign = self._require(campaign_id)
            self._require_not_terminal(campaign)
            if campaign.status in (CampaignStatus.PENDING, CampaignStatus.RUNNING):
                campaign.status = CampaignStatus.PAUSED
                campaign.updated_at = self._clock()
                self._persist()
            return campaign.to_dict()

    def resume(self, campaign_id: str) -> dict:
        with self._lock:
            campaign = self._require(campaign_id)
            self._require_not_terminal(campaign)
            if campaign.status is CampaignStatus.PAUSED:
                campaign.status = CampaignStatus.RUNNING
                campaign.updated_at = self._clock()
                self._persist()
        return self.run_until_blocked(campaign_id)

    def cancel(self, campaign_id: str) -> dict:
        with self._lock:
            campaign = self._require(campaign_id)
            self._require_not_terminal(campaign)
            campaign.status = CampaignStatus.CANCELLED
            for dev in campaign.device_ids:
                if campaign.device_states[dev] in (DeviceUpdateState.PENDING,
                                                   DeviceUpdateState.IN_PROGRESS):
                    campaign.device_states[dev] = DeviceUpdateState.SKIPPED
            campaign.updated_at = self._clock()
            self._persist()
            return campaign.to_dict()

    def status(self, campaign_id: str) -> dict:
        with self._lock:
            return self._require(campaign_id).to_dict()

    def list_campaigns(self) -> list[dict]:
        with self._lock:
            return [c.to_dict() for c in self._campaigns.values()]
