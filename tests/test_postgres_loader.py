import pytest
from datetime import datetime

from src.postgres_loader import format_load_result, parse_source_file_ts


class TestParseSourceFileTs:
    def test_valid_filename_returns_datetime(self):
        result = parse_source_file_ts("output_20260509_143022.csv")
        assert isinstance(result, datetime)

    def test_correct_date_parsed(self):
        result = parse_source_file_ts("output_20260509_143022.csv")
        assert result.year == 2026
        assert result.month == 5
        assert result.day == 9

    def test_correct_time_parsed(self):
        result = parse_source_file_ts("output_20260509_143022.csv")
        assert result.hour == 14
        assert result.minute == 30
        assert result.second == 22

    def test_full_path_accepted(self):
        result = parse_source_file_ts("/opt/airflow/data/raw/output_20260101_000000.csv")
        assert result == datetime(2026, 1, 1, 0, 0, 0)

    def test_missing_output_prefix_raises(self):
        with pytest.raises(ValueError, match="Unexpected file name format"):
            parse_source_file_ts("weather_20260509_143022.csv")

    def test_malformed_timestamp_raises(self):
        with pytest.raises(ValueError):
            parse_source_file_ts("output_not_a_date.csv")


class TestFormatLoadResult:
    def test_loaded_result_renders_counts_and_target(self):
        result = {
            "status": "loaded",
            "rows": 342,
            "source_file": "output_20260509_143022.csv",
            "table": "raw.weather_forecast",
        }
        assert format_load_result(result) == (
            "Loaded 342 rows from output_20260509_143022.csv into raw.weather_forecast"
        )

    def test_skipped_result_names_the_file(self):
        result = {
            "status": "skipped",
            "rows": 0,
            "source_file": "output_20260509_143022.csv",
            "table": "raw.weather_forecast",
        }
        assert format_load_result(result) == (
            "Skipped already loaded file: output_20260509_143022.csv"
        )

