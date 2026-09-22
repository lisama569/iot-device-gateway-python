"""uvicorn entry point: ``uvicorn main:app``."""
from gateway.app import app

__all__ = ["app"]
