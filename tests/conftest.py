import pytest


class FakeS3Client:
    # Stands in for boto3's S3 client: records each put_object call, no network.
    def __init__(self):
        self.calls = []

    def put_object(self, **kwargs):
        self.calls.append(kwargs)


@pytest.fixture
def fake_s3():
    return FakeS3Client()


@pytest.fixture
def minimal_response():
    return [
        {
            "city": {
                "id": 5128581,
                "name": "New York",
                "country": "US",
                "population": 8175133,
                "timezone": -14400,
                "sunrise": 1778270000,
                "sunset": 1778320000,
                "coord": {"lat": 40.7128, "lon": -74.006},
            },
            "list": [
                {
                    "dt": 1778284800,
                    "dt_txt": "2026-05-09 00:00:00",
                    "main": {
                        "temp": 60.51,
                        "feels_like": 58.57,
                        "temp_min": 59.0,
                        "temp_max": 62.0,
                        "humidity": 49,
                        "pressure": 1015,
                    },
                    "wind": {"speed": 7.07, "deg": 200, "gust": 9.1},
                    "clouds": {"all": 20},
                    "visibility": 10000,
                    "pop": 0.2,
                    "weather": [{"id": 500, "main": "Rain", "description": "light rain"}],
                }
            ],
        }
    ]
