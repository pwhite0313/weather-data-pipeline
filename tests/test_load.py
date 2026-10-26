import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from src.load import LINEAGE_COLUMNS, load_records, output_file_name, stamp_lineage
from src.schema import FORECAST_COLUMNS


@pytest.fixture
def records():
    return pd.DataFrame({"city_id": [1, 2], "main_temp": [60.5, 71.2]})


class TestAtomicWrite:
    def test_writes_a_readable_csv(self, records, tmp_path):
        target = tmp_path / "output_20260509_143022_000001.csv"
        load_records(records, target)
        assert pd.read_csv(target).shape == (2, 2)

    def test_returns_the_path_written(self, records, tmp_path):
        target = tmp_path / "output_20260509_143022_000001.csv"
        assert load_records(records, target) == str(target)

    def test_no_temp_file_is_left_behind(self, records, tmp_path):
        load_records(records, tmp_path / "output_20260509_143022_000001.csv")
        leftovers = [p.name for p in tmp_path.iterdir() if p.name.startswith(".")]
        assert leftovers == []

    def test_failed_write_leaves_no_partial_csv(self, tmp_path):
        # A frame that blows up during serialization must not leave a truncated
        # file at the target path, which is how the corrupted 601 row CSV that
        # broke a DAG run came to exist.
        class Exploding(pd.DataFrame):
            def to_csv(self, *args, **kwargs):
                raise RuntimeError("serialization failed")

        target = tmp_path / "output_20260509_143022_000001.csv"
        with pytest.raises(RuntimeError):
            load_records(Exploding({"a": [1]}), target)

        assert not target.exists()
        assert list(tmp_path.iterdir()) == []


class TestFilenameCollisions:
    def test_back_to_back_writes_do_not_collide(self, records, tmp_path, monkeypatch):
        # The regression that produced the corrupted file: two runs starting in
        # the same second resolved to one path and interleaved their writes.
        # Both calls here land in the same wall-clock second.
        import src.load as load_module

        monkeypatch.setattr(load_module, "RAW_DATA_DIR", tmp_path)

        first = load_records(records)
        second = load_records(records)

        assert first != second
        assert len(list(tmp_path.iterdir())) == 2

    def test_generated_filename_round_trips_through_the_parser(self, records, tmp_path, monkeypatch):
        # Whatever load.py writes, postgres_loader must be able to parse back.
        import src.load as load_module
        from src.postgres_loader import parse_source_file_ts

        monkeypatch.setattr(load_module, "RAW_DATA_DIR", tmp_path)

        written = load_records(records)
        assert parse_source_file_ts(written) is not None


class TestStampLineage:
    RUN_TS = datetime(2026, 10, 19, 18, 30, 0, 123456)

    def test_lineage_columns_are_appended_last_in_table_order(self, records):
        stamped = stamp_lineage(records, "output_x.csv", self.RUN_TS, "manual_1")
        assert list(stamped.columns) == ["city_id", "main_temp", *LINEAGE_COLUMNS]

    def test_values_come_from_the_one_run_timestamp(self, records):
        stamped = stamp_lineage(records, "output_x.csv", self.RUN_TS, "manual_1")
        row = stamped.iloc[0]
        assert row["source_file_name"] == "output_x.csv"
        assert row["source_file_ts"] == self.RUN_TS
        assert row["ingested_at"] == self.RUN_TS
        assert row["dag_run_id"] == "manual_1"

    def test_input_frame_is_not_modified(self, records):
        stamp_lineage(records, "output_x.csv", self.RUN_TS, "manual_1")
        assert list(records.columns) == ["city_id", "main_temp"]

    def test_restamping_keeps_one_set_of_columns_at_the_end(self, records):
        once = stamp_lineage(records, "output_x.csv", self.RUN_TS, "manual_1")
        twice = stamp_lineage(once, "output_y.csv", self.RUN_TS, "manual_2")
        assert list(twice.columns) == ["city_id", "main_temp", *LINEAGE_COLUMNS]
        assert twice.iloc[0]["source_file_name"] == "output_y.csv"


class TestOutputFileName:
    def test_uses_the_microsecond_format(self):
        name = output_file_name(datetime(2026, 10, 19, 18, 30, 0, 123456))
        assert name == "output_20261019_183000_123456.csv"


class TestCsvMatchesSnowflakeRawTable:
    # COPY maps CSV columns to RAW.WEATHER_FORECAST by position, so the CSV's
    # columns must match the table's, names and order, not just names.
    def test_csv_columns_match_setup_05_raw_tables(self):
        sql = (Path(__file__).resolve().parents[1] / "setup" / "05_raw_tables.sql").read_text()
        body = re.search(r"RAW\.WEATHER_FORECAST \((.*?)\);", sql, re.S).group(1)
        table_columns = [line.split()[0].lower() for line in body.strip().splitlines()]
        assert [*FORECAST_COLUMNS, *LINEAGE_COLUMNS] == table_columns
