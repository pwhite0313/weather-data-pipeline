import pandas as pd
import pytest

from src.load import load_records


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
