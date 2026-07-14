"""Benchmark workload plan and competitor availability reporting."""

from __future__ import annotations

import argparse
import importlib.metadata
import importlib.util
import json
import platform
import sys
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class Competitor:
    """External queue library expected in competitive benchmark environments."""

    name: str
    import_name: str
    distribution: str


@dataclass(frozen=True)
class Workload:
    """Canonical workload dimensions used to keep benchmark runs comparable."""

    name: str
    jobs: int
    payload_bytes: int
    concurrency: int
    workers: int
    queue_depth: int
    warmup_jobs: int
    description: str


COMPETITORS: tuple[Competitor, ...] = (
    Competitor(name="Celery", import_name="celery", distribution="celery"),
    Competitor(name="Dramatiq", import_name="dramatiq", distribution="dramatiq"),
    Competitor(name="Arq", import_name="arq", distribution="arq"),
    Competitor(name="RQ", import_name="rq", distribution="rq"),
    Competitor(name="Huey", import_name="huey", distribution="huey"),
)

WORKLOADS: tuple[Workload, ...] = (
    Workload(
        name="small-payload",
        jobs=10_000,
        payload_bytes=128,
        concurrency=10,
        workers=1,
        queue_depth=10_000,
        warmup_jobs=1_000,
        description="Short JSON-like payloads for enqueue/dequeue latency and steady throughput.",
    ),
    Workload(
        name="large-payload",
        jobs=2_000,
        payload_bytes=256_000,
        concurrency=10,
        workers=1,
        queue_depth=2_000,
        warmup_jobs=100,
        description="Large payload pressure for serialization, broker bandwidth, and memory behavior.",
    ),
    Workload(
        name="worker-fanout-100",
        jobs=100_000,
        payload_bytes=128,
        concurrency=10,
        workers=100,
        queue_depth=100_000,
        warmup_jobs=1_000,
        description="Horizontal worker fanout at the first production-scale target.",
    ),
    Workload(
        name="worker-fanout-1000",
        jobs=1_000_000,
        payload_bytes=128,
        concurrency=10,
        workers=1_000,
        queue_depth=1_000_000,
        warmup_jobs=10_000,
        description="Upper fanout target for autoscaling and broker contention analysis.",
    ),
)

MEASUREMENT_POLICY: dict[str, Any] = {
    "clock": "time.perf_counter_ns",
    "warmup": "Run warmup_jobs before collecting timed samples for each workload.",
    "minimum_repetitions": 5,
    "statistics": ["median", "p95", "p99", "min", "max"],
    "metrics": [
        "enqueue_latency_ns",
        "dequeue_latency_ns",
        "completion_throughput_jobs_per_second",
        "cpu_percent",
        "rss_bytes",
        "queue_depth",
        "recovery_time_ns",
    ],
}


def competitor_availability() -> list[dict[str, Any]]:
    """Return import and package-version availability for expected competitors."""

    availability: list[dict[str, Any]] = []
    for competitor in COMPETITORS:
        spec = importlib.util.find_spec(competitor.import_name)
        version: str | None = None
        reason: str | None = None
        installed = spec is not None
        if installed:
            try:
                version = importlib.metadata.version(competitor.distribution)
            except importlib.metadata.PackageNotFoundError:
                reason = "module importable but distribution metadata is unavailable"
        else:
            reason = "not installed"
        availability.append(
            {
                "name": competitor.name,
                "import_name": competitor.import_name,
                "distribution": competitor.distribution,
                "installed": installed,
                "version": version,
                "reason": reason,
            }
        )
    return availability


def benchmark_plan() -> dict[str, Any]:
    """Build a JSON-serializable benchmark plan for local and CI runs."""

    return {
        "python": {
            "version": sys.version.split()[0],
            "implementation": platform.python_implementation(),
        },
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
        },
        "measurement_policy": MEASUREMENT_POLICY,
        "workloads": [asdict(workload) for workload in WORKLOADS],
        "competitors": competitor_availability(),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Print the AsyncMQ benchmark plan.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument(
        "--require-competitors",
        action="store_true",
        help="Exit non-zero when any expected competitor library is not importable.",
    )
    args = parser.parse_args(argv)

    plan = benchmark_plan()
    if args.json:
        print(json.dumps(plan, indent=2, sort_keys=True))
    else:
        print("AsyncMQ benchmark plan")
        print(f"Python: {plan['python']['implementation']} {plan['python']['version']}")
        print(f"Platform: {plan['platform']['system']} {plan['platform']['machine']}")
        print("Workloads:")
        for workload in plan["workloads"]:
            print(
                "  - {name}: jobs={jobs} payload_bytes={payload_bytes} "
                "workers={workers} concurrency={concurrency}".format(**workload)
            )
        print("Competitors:")
        for competitor in plan["competitors"]:
            status = "installed" if competitor["installed"] else competitor["reason"]
            version = f" ({competitor['version']})" if competitor["version"] else ""
            print(f"  - {competitor['name']}: {status}{version}")

    if args.require_competitors:
        missing = [item["name"] for item in plan["competitors"] if not item["installed"]]
        if missing:
            print(f"Missing competitor libraries: {', '.join(missing)}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
