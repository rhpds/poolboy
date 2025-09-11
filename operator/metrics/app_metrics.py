from __future__ import annotations

from aioprometheus import REGISTRY, Counter, Histogram


class AppMetrics:
    registry = REGISTRY

    process_time = Histogram(
        "poolboy_process_time_seconds",
        "Execution time of processes in the app",
        {
            "method": "The method name",
            "status": "The status of the request",
            "app": "The application name",
            "cluster_domain": "The cluster name",
        },
        registry=registry,
    )

    invalid_resource_counter = Counter(
        "poolboy_invalid_resource_count",
        "Counts the number of resources in invalid states",
        registry=registry,
    )
