from datetime import date, datetime

import pytest

from src.s3 import s3_key, upload_bytes


class TestS3Key:
    def test_raw_key(self):
        key = s3_key("raw", date(2026, 10, 19), "5128581_20261019_183000_123456.json")
        assert key == "weather/raw/dt=2026-10-19/5128581_20261019_183000_123456.json"

    def test_flat_key(self):
        key = s3_key("flat", date(2026, 10, 19), "output_20261019_183000_123456.csv")
        assert key == "weather/flat/dt=2026-10-19/output_20261019_183000_123456.csv"

    def test_datetime_keeps_only_the_date(self):
        key = s3_key("flat", datetime(2026, 10, 19, 23, 59), "output.csv")
        assert key == "weather/flat/dt=2026-10-19/output.csv"

    def test_unknown_kind_is_rejected(self):
        with pytest.raises(ValueError):
            s3_key("flatt", date(2026, 10, 19), "output.csv")


class TestUploadBytes:
    def test_passes_everything_to_put_object(self, fake_s3):
        upload_bytes(fake_s3, "bucket", "weather/raw/dt=2026-10-19/x.json", b"{}", "application/json")
        assert fake_s3.calls == [{
            "Bucket": "bucket",
            "Key": "weather/raw/dt=2026-10-19/x.json",
            "Body": b"{}",
            "ContentType": "application/json",
        }]
