import pytest

from src import quality_checks
from src.quality_checks import (
    MIN_RUNS_FOR_CHECK,
    check_volume_anomaly,
    evaluate_volume_anomaly,
)


class TestEvaluateVolumeAnomalyHistory:
    def test_no_history_is_not_checked(self):
        result = evaluate_volume_anomaly(100, [])
        assert result["checked"] is False
        assert result["reason"] == "insufficient_history"

    def test_below_minimum_history_is_not_checked(self):
        recent = [100] * (MIN_RUNS_FOR_CHECK - 1)
        result = evaluate_volume_anomaly(1, recent)
        assert result["checked"] is False
        assert result["runs_available"] == MIN_RUNS_FOR_CHECK - 1

    def test_minimum_history_is_checked(self):
        recent = [100] * MIN_RUNS_FOR_CHECK
        result = evaluate_volume_anomaly(100, recent)
        assert result["checked"] is True


class TestEvaluateVolumeAnomalyThreshold:
    def test_normal_volume_is_not_anomalous(self):
        result = evaluate_volume_anomaly(100, [100, 100, 100])
        assert result["anomalous"] is False

    def test_far_below_average_is_anomalous(self):
        result = evaluate_volume_anomaly(10, [100, 100, 100])
        assert result["anomalous"] is True

    def test_higher_than_average_is_not_anomalous(self):
        result = evaluate_volume_anomaly(500, [100, 100, 100])
        assert result["anomalous"] is False

    def test_exactly_at_threshold_is_not_anomalous(self):
        # Threshold is strict less-than, so a run sitting exactly on the
        # boundary passes rather than failing the pipeline on a rounding edge.
        result = evaluate_volume_anomaly(50, [100, 100, 100])
        assert result["threshold"] == 50.0
        assert result["anomalous"] is False

    def test_one_row_below_threshold_is_anomalous(self):
        result = evaluate_volume_anomaly(49, [100, 100, 100])
        assert result["anomalous"] is True

    def test_zero_rows_is_anomalous(self):
        result = evaluate_volume_anomaly(0, [100, 100, 100])
        assert result["anomalous"] is True

    def test_rolling_average_uses_all_supplied_runs(self):
        result = evaluate_volume_anomaly(100, [100, 200, 300])
        assert result["rolling_avg"] == 200.0
        assert result["threshold"] == 100.0


class TestEvaluateVolumeAnomalyEdgeCases:
    def test_all_zero_history_never_flags(self):
        # An empty warehouse gives a zero threshold, so nothing can fall below
        # it. The check should stay quiet rather than divide-by-zero or flag.
        result = evaluate_volume_anomaly(0, [0, 0, 0])
        assert result["rolling_avg"] == 0.0
        assert result["anomalous"] is False

    def test_result_reports_current_count(self):
        result = evaluate_volume_anomaly(77, [100, 100, 100])
        assert result["current_count"] == 77


class TestCheckVolumeAnomaly:
    """The raising wrapper, with the database read stubbed out."""

    def _stub_volumes(self, monkeypatch, current, recent):
        monkeypatch.setattr(
            quality_checks,
            "fetch_run_volumes",
            lambda engine, run_id: (current, recent),
        )

    def test_anomalous_run_raises(self, monkeypatch):
        self._stub_volumes(monkeypatch, 10, [100, 100, 100])
        with pytest.raises(ValueError, match="Volume anomaly detected"):
            check_volume_anomaly(engine=None, current_run_id="manual__run_1")

    def test_raised_message_names_the_run(self, monkeypatch):
        self._stub_volumes(monkeypatch, 10, [100, 100, 100])
        with pytest.raises(ValueError, match="manual__run_1"):
            check_volume_anomaly(engine=None, current_run_id="manual__run_1")

    def test_normal_run_returns_result(self, monkeypatch):
        self._stub_volumes(monkeypatch, 100, [100, 100, 100])
        result = check_volume_anomaly(engine=None, current_run_id="manual__run_1")
        assert result["anomalous"] is False

    def test_insufficient_history_does_not_raise(self, monkeypatch):
        self._stub_volumes(monkeypatch, 1, [100])
        result = check_volume_anomaly(engine=None, current_run_id="manual__run_1")
        assert result["checked"] is False
