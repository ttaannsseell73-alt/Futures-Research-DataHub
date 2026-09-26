import pytest

from datahub.coverage import coverage
from datahub.planning import (
    DAY,
    coverage_matrix,
    create_backfill_plan,
    plan_status,
    run_plan,
)
from datahub.schemas import normalize
from datahub.storage import Store
from datahub.universe import import_lifecycle

START = 1_704_067_200_000


def lifecycle(store, days=3):
    evidence = {
        "source": "audited_fixture",
        "complete": True,
        "coverage_start": START,
        "coverage_end": START + days * DAY,
        "records": [
            {
                "symbol": "BTCUSDT",
                "listed_at": START,
                "delisted_at": None,
                "known_at": START,
                "source": "fixture",
            }
        ],
    }
    return import_lifecycle(store, evidence)


def rows(start, end, step):
    return [[t, "10", "12", "9", "11", "2"] for t in range(start, end, step)]


def test_coverage_reports_gap_then_complete(tmp_path):
    store = Store(tmp_path)
    half = START + DAY // 2
    store.put(
        normalize(rows(START, half, 60_000), "ohlcv"),
        "ohlcv",
        "BTCUSDT",
        "1m",
        START,
        half,
        {"source": "fixture"},
    )
    report = coverage(store, "ohlcv", "BTCUSDT", "1m", START, START + DAY)
    assert report["status"] == "GAPPED"
    assert report["gaps"] == [[half, START + DAY]]
    assert report["coverage_ratio"] == 0.5

    store.put(
        normalize(rows(half, START + DAY, 60_000), "ohlcv"),
        "ohlcv",
        "BTCUSDT",
        "1m",
        half,
        START + DAY,
        {"source": "fixture"},
    )
    report = coverage(store, "ohlcv", "BTCUSDT", "1m", START, START + DAY)
    assert report["status"] == "COMPLETE"
    assert report["gaps"] == []


def test_funding_coverage_never_claims_completeness(tmp_path):
    store = Store(tmp_path)
    table = normalize(
        [
            {"fundingTime": START, "fundingRate": "0.001"},
            {"fundingTime": START + 8 * 3_600_000, "fundingRate": "-0.001"},
        ],
        "funding",
    )
    store.put(table, "funding", "BTCUSDT", "1m", START, START + DAY, {"source": "fixture"})
    report = coverage(store, "funding", "BTCUSDT", "1m", START, START + DAY)
    assert report["status"] == "EVENT_STREAM"
    assert report["complete"] is None
    assert report["gaps"] is None


def test_backfill_plan_uses_vision_for_full_days_and_rest_for_edges(tmp_path):
    store = Store(tmp_path)
    lid = lifecycle(store)
    full = create_backfill_plan(
        store,
        "full_day",
        lid,
        ["ohlcv"],
        ["1m"],
        START,
        START + DAY,
    )
    assert len(full["jobs"]) == 1
    assert full["jobs"][0]["source"] == "vision"

    partial = create_backfill_plan(
        store,
        "partial_day",
        lid,
        ["ohlcv"],
        ["1m"],
        START + 60_000,
        START + DAY,
    )
    assert len(partial["jobs"]) == 1
    assert partial["jobs"][0]["source"] == "rest"

    with pytest.raises(ValueError, match="partial UTC days"):
        create_backfill_plan(
            store,
            "vision_partial",
            lid,
            ["ohlcv"],
            ["1m"],
            START + 60_000,
            START + DAY,
            "vision",
        )


def test_funding_plan_is_rest_once_per_symbol_interval(tmp_path):
    store = Store(tmp_path)
    lid = lifecycle(store)
    plan = create_backfill_plan(
        store,
        "funding",
        lid,
        ["funding"],
        ["1m", "5m", "1h"],
        START,
        START + DAY,
    )
    assert len(plan["jobs"]) == 1
    assert plan["jobs"][0]["source"] == "rest"
    assert plan["jobs"][0]["timeframe"] == "1m"


def test_plan_is_immutable_and_coverage_matrix_uses_lifecycle(tmp_path):
    store = Store(tmp_path)
    lid = lifecycle(store)
    first = create_backfill_plan(
        store,
        "immutable",
        lid,
        ["ohlcv"],
        ["4h"],
        START,
        START + DAY,
    )
    assert (
        create_backfill_plan(
            store,
            "immutable",
            lid,
            ["ohlcv"],
            ["4h"],
            START,
            START + DAY,
        )
        == first
    )
    with pytest.raises(ValueError, match="Immutable"):
        create_backfill_plan(
            store,
            "immutable",
            lid,
            ["ohlcv"],
            ["1h"],
            START,
            START + DAY,
        )
    matrix = coverage_matrix(store, lid, ["ohlcv"], ["4h"], START, START + DAY)
    assert matrix["status_counts"] == {"GAPPED": 1}
    assert matrix["rows"][0]["symbol"] == "BTCUSDT"


def test_runner_resumes_only_failed_jobs(tmp_path):
    class Adapter:
        name = "rest"

        def __init__(self):
            self.calls = []
            self.failed_once = False

        def fetch(self, kind, symbol, timeframe, start, end):
            self.calls.append(start)
            if start == START + DAY and not self.failed_once:
                self.failed_once = True
                raise RuntimeError("fixture interruption")
            step = 14_400_000
            return normalize(rows(start, end, step), kind), {"source": "fixture"}

    store = Store(tmp_path)
    lid = lifecycle(store)
    plan = create_backfill_plan(
        store,
        "resume",
        lid,
        ["ohlcv"],
        ["4h"],
        START,
        START + 2 * DAY,
        "rest",
    )
    assert len(plan["jobs"]) == 2
    adapter = Adapter()
    first = run_plan(store, "resume", adapters={"rest": adapter})
    assert first["status"] == "INCOMPLETE"
    assert first["counts"] == {"PENDING": 0, "COMPLETE": 1, "FAILED": 1}
    second = run_plan(store, "resume", adapters={"rest": adapter})
    assert second["status"] == "COMPLETE"
    assert second["counts"] == {"PENDING": 0, "COMPLETE": 2, "FAILED": 0}
    assert adapter.calls.count(START) == 1
    assert adapter.calls.count(START + DAY) == 2
    assert plan_status(store, "resume")["status"] == "COMPLETE"


def test_plan_includes_historical_delisted_symbols(tmp_path):
    store = Store(tmp_path)
    evidence = {
        "source": "audited_fixture",
        "complete": True,
        "coverage_start": START,
        "coverage_end": START + 2 * DAY,
        "records": [
            {
                "symbol": "DEADUSDT",
                "listed_at": START - DAY,
                "delisted_at": START + DAY,
                "known_at": START,
                "source": "fixture",
            },
            {
                "symbol": "LIVEUSDT",
                "listed_at": START + DAY,
                "delisted_at": None,
                "known_at": START,
                "source": "fixture",
            },
        ],
    }
    lid = import_lifecycle(store, evidence)
    plan = create_backfill_plan(
        store,
        "survivorship_safe",
        lid,
        ["ohlcv"],
        ["1h"],
        START,
        START + 2 * DAY,
    )
    assert {job["symbol"] for job in plan["jobs"]} == {"DEADUSDT", "LIVEUSDT"}
    assert len(plan["jobs"]) == 2
