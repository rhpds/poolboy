"""Prometheus metrics definitions for Poolboy."""

from __future__ import annotations

from prometheus_client import REGISTRY, Counter, Histogram


class AppMetrics:
    """Central registry for all application metrics."""

    registry = REGISTRY

    process_time = Histogram(
        "poolboy_process_time_seconds",
        "Execution time of processes in the app",
        ["method", "status", "app", "cluster_domain"],
        registry=REGISTRY,
    )

    invalid_resource_counter = Counter(
        "poolboy_invalid_resource_count",
        "Counts the number of resources in invalid states",
        ["resource_type", "cluster_domain"],
        registry=REGISTRY,
    )
