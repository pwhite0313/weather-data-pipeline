import json
from datetime import date, datetime

import pandas as pd

import pytest

from src.landing import bucket_name, upload_flat_csv, upload_flat_frame, upload_raw_payloads

RUN_TS = datetime(2026, 10, 19, 18, 30, 0, 123456)


def payload(city_id):
    return {"city": {"id": city_id, "name": "x"}, "list": [{"dt": 1}]}


class TestUploadRawPayloads:
    def test_one_object_per_city_named_by_city_id_and_run_ts(self, fake_s3):
        keys = upload_raw_payloads(fake_s3, "bucket", [payload(5128581), payload(4887398)], RUN_TS)
        assert keys == [
            "weather/raw/dt=2026-10-19/5128581_20261019_183000_123456.json",
            "weather/raw/dt=2026-10-19/4887398_20261019_183000_123456.json",
        ]

    def test_body_is_the_payload_as_json(self, fake_s3):
        upload_raw_payloads(fake_s3, "bucket", [payload(5128581)], RUN_TS)
        call = fake_s3.calls[0]
        assert json.loads(call["Body"]) == payload(5128581)
        assert call["ContentType"] == "application/json"


class TestUploadFlatCsv:
    def test_uploads_the_file_under_its_own_name(self, fake_s3, tmp_path):
        csv = tmp_path / "output_20261019_183000_123456.csv"
        csv.write_text("a,b\n1,2\n")
        key = upload_flat_csv(fake_s3, "bucket", csv, RUN_TS)
        assert key == "weather/flat/dt=2026-10-19/output_20261019_183000_123456.csv"
        assert fake_s3.calls[0]["Body"] == b"a,b\n1,2\n"
        assert fake_s3.calls[0]["ContentType"] == "text/csv"


class TestBucketName:
    def test_missing_bucket_fails_loudly(self, monkeypatch):
        monkeypatch.delenv("S3_BUCKET", raising=False)
        with pytest.raises(RuntimeError, match="S3_BUCKET"):
            bucket_name()


class TestPartitionDateFromTheRun:
    def test_dt_overrides_the_partition_but_not_the_file_timestamp(self, fake_s3):
        late_write = datetime(2026, 10, 28, 0, 5, 0, 1)
        keys = upload_raw_payloads(fake_s3, "bucket", [payload(5128581)], late_write, dt=date(2026, 10, 27))
        assert keys == ["weather/raw/dt=2026-10-27/5128581_20261028_000500_000001.json"]


class TestUploadFlatFrame:
    def test_writes_csv_bytes_with_header_and_no_index(self, fake_s3):
        df = pd.DataFrame({"city_id": [1, 2], "main_temp": [60.5, None]})
        key = upload_flat_frame(fake_s3, "bucket", df, "output_x.csv", date(2026, 10, 27))
        assert key == "weather/flat/dt=2026-10-27/output_x.csv"
        assert fake_s3.calls[0]["Body"] == b"city_id,main_temp\n1,60.5\n2,\n"
        assert fake_s3.calls[0]["ContentType"] == "text/csv"
