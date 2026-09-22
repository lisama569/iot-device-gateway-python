"""Firmware upgrade orchestration HTTP API."""
from __future__ import annotations

from fastapi import APIRouter, Request

from ..models import CreateJobRequest, JobControlRequest, JobView

router = APIRouter(prefix="/upgrades", tags=["upgrades"])


@router.post("", response_model=JobView, status_code=201)
async def create_job(payload: CreateJobRequest, request: Request):
    svc = request.app.state.registry.firmware_service
    view = await svc.create_job(payload)
    await svc.start_job(view.job_id)
    return await svc.get_job(view.job_id)


@router.get("/{job_id}", response_model=JobView)
async def get_job(job_id: str, request: Request):
    return await request.app.state.registry.firmware_service.get_job(job_id)


@router.post("/{job_id}/pause", response_model=JobView)
async def pause_job(job_id: str, payload: JobControlRequest, request: Request):
    return await request.app.state.registry.firmware_service.pause_job(
        job_id, payload.reason
    )


@router.post("/{job_id}/resume", response_model=JobView)
async def resume_job(job_id: str, request: Request):
    return await request.app.state.registry.firmware_service.resume_job(job_id)


@router.post("/{job_id}/cancel", response_model=JobView)
async def cancel_job(job_id: str, payload: JobControlRequest, request: Request):
    return await request.app.state.registry.firmware_service.cancel_job(
        job_id, payload.reason
    )
