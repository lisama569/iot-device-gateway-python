"""Device registration / lifecycle HTTP API."""
from __future__ import annotations

from fastapi import APIRouter, Request

from ..models import DeviceView, RegisterDeviceRequest, RotateCredentialRequest

router = APIRouter(prefix="/devices", tags=["devices"])


def _service(request: Request):
    return request.app.state.registry.device_service


class CredentialResponse(DeviceView):
    credential_secret: str


@router.post("", response_model=DeviceView, status_code=201)
async def register_device(payload: RegisterDeviceRequest, request: Request):
    view, _secret = await _service(request).register(payload)
    return view


@router.post("/register", response_model=CredentialResponse, status_code=201)
async def register_with_secret(payload: RegisterDeviceRequest, request: Request):
    """Idempotent registration that also returns the credential secret."""
    view, secret = await _service(request).register(payload)
    return CredentialResponse(**view.model_dump(), credential_secret=secret)


@router.get("/{device_id}", response_model=DeviceView)
async def get_device(device_id: str, request: Request):
    return await _service(request).get(device_id)


@router.post("/{device_id}/enable", response_model=DeviceView)
async def enable_device(device_id: str, request: Request):
    return await _service(request).set_enabled(device_id, True)


@router.post("/{device_id}/disable", response_model=DeviceView)
async def disable_device(device_id: str, request: Request):
    return await _service(request).set_enabled(device_id, False)


@router.post("/{device_id}/deregister", response_model=DeviceView)
async def deregister_device(device_id: str, request: Request):
    return await _service(request).deregister(device_id)


@router.post("/{device_id}/credentials/rotate", response_model=CredentialResponse)
async def rotate_credential(device_id: str, payload: RotateCredentialRequest, request: Request):
    view, secret = await _service(request).rotate_credential(device_id, payload)
    return CredentialResponse(**view.model_dump(), credential_secret=secret)


@router.post("/{device_id}/credentials/{credential_id}/revoke", response_model=DeviceView)
async def revoke_credential(device_id: str, credential_id: str, request: Request):
    return await _service(request).revoke_credential(device_id, credential_id)
