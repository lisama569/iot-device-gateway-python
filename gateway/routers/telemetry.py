"""Telemetry ingestion / shadow HTTP API."""
from __future__ import annotations

from fastapi import APIRouter, Request

from ..models import ShadowView, TelemetryMessage, TelemetryResult

router = APIRouter(prefix="/telemetry", tags=["telemetry"])


@router.post("", response_model=TelemetryResult)
async def ingest(payload: TelemetryMessage, request: Request):
    svc = request.app.state.registry.telemetry_service
    return await svc.ingest(payload)


@router.get("/{device_id}/shadow", response_model=ShadowView)
async def get_shadow(device_id: str, request: Request):
    svc = request.app.state.registry.telemetry_service
    return await svc.get_shadow(device_id)
