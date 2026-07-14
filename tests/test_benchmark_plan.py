import json

from benchmarks.plan import COMPETITORS, WORKLOADS, benchmark_plan, competitor_availability


def test_benchmark_plan_declares_required_competitors():
    names = {competitor.name for competitor in COMPETITORS}

    assert names == {"Celery", "Dramatiq", "Arq", "RQ", "Huey"}


def test_benchmark_plan_covers_production_scale_workloads():
    workloads = {workload.name: workload for workload in WORKLOADS}

    assert workloads["worker-fanout-100"].workers == 100
    assert workloads["worker-fanout-100"].queue_depth == 100_000
    assert workloads["worker-fanout-1000"].workers == 1_000
    assert workloads["worker-fanout-1000"].queue_depth == 1_000_000


def test_competitor_availability_reports_missing_libraries_without_importing_them():
    availability = competitor_availability()

    assert {item["name"] for item in availability} == {"Celery", "Dramatiq", "Arq", "RQ", "Huey"}
    for item in availability:
        assert set(item) == {
            "name",
            "import_name",
            "distribution",
            "installed",
            "version",
            "reason",
        }


def test_benchmark_plan_is_json_serializable():
    plan = benchmark_plan()

    encoded = json.dumps(plan)

    assert "measurement_policy" in plan
    assert "small-payload" in encoded
