import pytest

from src.alerts import failure_message, notify_failure


class FakeResponse:
    def __init__(self, ok=True):
        self.ok = ok

    def raise_for_status(self):
        if not self.ok:
            raise RuntimeError("503 from ntfy")


class Recorder:
    def __init__(self, ok=True, boom=False):
        self.calls = []
        self.ok = ok
        self.boom = boom

    def __call__(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if self.boom:
            raise ConnectionError("no network")
        return FakeResponse(self.ok)


class TestFailureMessage:
    def test_names_dag_task_and_try(self):
        assert failure_message("weather_daily", "transform", 3, None) == "weather_daily.transform try 3 failed"

    def test_log_url_on_its_own_line(self):
        msg = failure_message("weather_daily", "transform", 1, "http://box/log")
        assert msg == "weather_daily.transform try 1 failed\nhttp://box/log"


class TestNotifyFailure:
    def test_posts_to_the_topic_from_env(self, monkeypatch):
        monkeypatch.setenv("NTFY_TOPIC", "weather-abc")
        post = Recorder()
        assert notify_failure("weather_daily", "transform", 1, "http://box/log", post=post) is True
        url, kwargs = post.calls[0]
        assert url == "https://ntfy.sh/weather-abc"
        assert kwargs["data"] == b"weather_daily.transform try 1 failed\nhttp://box/log"
        assert kwargs["headers"]["Title"] == "weather_daily failed"
        assert kwargs["timeout"] == 10

    def test_no_topic_means_no_post_and_false(self, monkeypatch):
        monkeypatch.delenv("NTFY_TOPIC", raising=False)
        post = Recorder()
        assert notify_failure("weather_daily", "transform", 1, None, post=post) is False
        assert post.calls == []

    def test_network_error_is_swallowed(self, monkeypatch):
        monkeypatch.setenv("NTFY_TOPIC", "weather-abc")
        assert notify_failure("weather_daily", "transform", 1, None, post=Recorder(boom=True)) is False

    def test_http_error_is_swallowed(self, monkeypatch):
        monkeypatch.setenv("NTFY_TOPIC", "weather-abc")
        assert notify_failure("weather_daily", "transform", 1, None, post=Recorder(ok=False)) is False
