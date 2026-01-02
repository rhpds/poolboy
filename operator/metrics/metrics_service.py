"""Prometheus metrics HTTP server with multiprocess support."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from threading import Thread
from wsgiref.simple_server import WSGIRequestHandler, make_server

from prometheus_client import REGISTRY, CollectorRegistry, multiprocess
from prometheus_client.exposition import ThreadingWSGIServer, make_wsgi_app
from prometheus_client.multiprocess import MultiProcessCollector

logger = logging.getLogger(__name__)


class MetricsService:
    """HTTP server that exposes Prometheus metrics on /metrics endpoint."""

    _server = None
    _thread = None
    _multiproc_dir: Path | None = None

    @classmethod
    def _get_registry(cls) -> CollectorRegistry:
        """Return the appropriate collector registry based on environment."""
        multiproc_dir = os.environ.get("PROMETHEUS_MULTIPROC_DIR")

        if multiproc_dir:
            cls._multiproc_dir = Path(multiproc_dir)
            cls._multiproc_dir.mkdir(parents=True, exist_ok=True)

            registry = CollectorRegistry()
            MultiProcessCollector(registry)
            logger.info(f"Using multiprocess registry: {multiproc_dir}")
            return registry

        logger.info("Using single-process registry")
        return REGISTRY

    @classmethod
    def start(cls, port: int = 9090, addr: str = "0.0.0.0") -> None:
        """Start the metrics server in a background daemon thread."""
        registry = cls._get_registry()

        app = make_wsgi_app(registry)
        cls._server = make_server(
            addr, port, app, ThreadingWSGIServer,
            handler_class=_SilentHandler
        )

        cls._thread = Thread(target=cls._server.serve_forever, daemon=True)
        cls._thread.start()
        logger.info(f"Metrics server started on {addr}:{port}")

    @classmethod
    def stop(cls) -> None:
        """Stop the metrics server and cleanup resources."""
        if cls._server:
            cls._server.shutdown()
            logger.info("Metrics server stopped")

        cls._cleanup_multiproc()

    @classmethod
    def _cleanup_multiproc(cls) -> None:
        """Mark current process as dead for multiprocess cleanup."""
        if cls._multiproc_dir and cls._multiproc_dir.exists():
            try:
                pid = os.getpid()
                multiprocess.mark_process_dead(pid)
                logger.debug(f"Marked process {pid} as dead")
            except Exception as e:
                logger.warning(f"Error cleaning up multiproc: {e}")


class _SilentHandler(WSGIRequestHandler):
    """WSGI request handler that suppresses access logs."""

    def log_message(self, format, *args):
        pass
