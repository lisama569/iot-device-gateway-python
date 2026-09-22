"""Local simulated device firmware agent.

No real devices are involved. ``simulate_upgrade`` reports a
success/failure for one dispatch attempt:

* a per-(device, version) deterministic RNG decides whether the
  *attempt* fails, seeded from stable identifiers so test runs are
  reproducible and a *retry* can succeed where the first attempt failed;
* once a version is installed, re-dispatching it returns an idempotent
  success (defence against duplicate pushes after a crash);
* ``force_fail_versions`` lets tests mark specific firmware versions as
  permanently failing on a device (used for job-level failure tests).
"""
from __future__ import annotations

import asyncio
import hashlib
import random


class DeviceSimulator:
    def __init__(self, fail_rate: float = 0.2, delay: float = 0.01) -> None:
        self.fail_rate = fail_rate
        self.delay = delay
        # device_id -> set of firmware versions already "installed":
        # redelivering an already-installed version succeeds idempotently.
        self._installed: dict[str, set[str]] = {}
        # (device_id, version) -> permanent failure
        self.force_fail: set[tuple[str, str]] = set()

    async def simulate_upgrade(
        self, device_id: str, firmware_version: str, attempt: int
    ) -> tuple[bool, str | None]:
        """Perform one simulated push. Returns ``(ok, error)``."""
        if self.delay:
            await asyncio.sleep(self.delay)

        installed = self._installed.setdefault(device_id, set())
        if firmware_version in installed:
            return True, None

        if (device_id, firmware_version) in self.force_fail:
            return False, f"simulated permanent failure installing {firmware_version}"

        # Deterministic per device+version+attempt: attempt 1 may fail,
        # a retry draws a fresh sample and can succeed.
        seed_src = f"{device_id}|{firmware_version}|{attempt}".encode("utf-8")
        seed = int.from_bytes(hashlib.sha256(seed_src).digest()[:8], "big")
        rng = random.Random(seed)
        if rng.random() < self.fail_rate:
            return False, f"simulated transient failure on attempt {attempt}"

        installed.add(firmware_version)
        return True, None

    def installed_versions(self, device_id: str) -> set[str]:
        return set(self._installed.get(device_id, ()))

    def force_failure(self, device_id: str, firmware_version: str) -> None:
        self.force_fail.add((device_id, firmware_version))

    def mark_installed(self, device_id: str, firmware_version: str) -> None:
        self._installed.setdefault(device_id, set()).add(firmware_version)

    def reset(self) -> None:
        self._installed.clear()
        self.force_fail.clear()
