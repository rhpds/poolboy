"""
Celery processor module for Poolboy.

Imports should be done directly from submodules to avoid circular imports:
    from processor.app import app, WorkerState, is_worker_enabled

This __init__.py intentionally does NOT import from .app to prevent
circular import issues when tasks import processor components.
"""

__all__ = ['app', 'config']
