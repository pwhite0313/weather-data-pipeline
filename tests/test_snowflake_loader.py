import pytest
from snowflake.connector.errors import ProgrammingError

from src.snowflake_loader import LoadError, LoadResult, copy_flat, copy_sql, stage_path

# COPY INTO's result columns, as Snowflake names them, when at least one file
# was processed.
COPY_COLUMNS = (
    "file", "status", "rows_parsed", "rows_loaded", "error_limit", "errors_seen",
    "first_error", "first_error_line", "first_error_character", "first_error_column_name",
)


class FakeCursor:
    """Records the SQL it is given and returns canned results."""

    def __init__(self, columns=(), rows=(), error=None):
        self.description = [(c,) for c in columns]
        self._rows = list(rows)
        self._error = error
        self.sfqid = "01b0-fake-query-id"
        self.executed = []
        self.closed = False

    def execute(self, sql):
        self.executed.append(sql)
        if self._error is not None:
            raise self._error

    def fetchall(self):
        return self._rows

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def loaded_row(name, parsed, loaded, errors=0):
    return (name, "LOADED", parsed, loaded, 1, errors, None, None, None, None)


class TestStagePath:
    def test_iso_date_becomes_a_partition_path(self):
        assert stage_path("2026-10-27") == "@RAW.S3_FLAT_STAGE/dt=2026-10-27/"

    def test_bad_date_raises_before_any_sql(self):
        with pytest.raises(ValueError):
            stage_path("2026-13-45")

    def test_injection_attempt_raises(self):
        with pytest.raises(ValueError):
            stage_path("2026-10-27/ ; DROP TABLE RAW.WEATHER_FORECAST")


class TestCopySql:
    def test_names_stage_format_and_date(self):
        sql = copy_sql("2026-10-27")
        assert "COPY INTO RAW.WEATHER_FORECAST" in sql
        assert "FROM @RAW.S3_FLAT_STAGE/dt=2026-10-27/" in sql
        assert "FORMAT_NAME = RAW.CSV_PARSE_HEADER" in sql
        assert "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE" in sql
        assert "ON_ERROR = ABORT_STATEMENT" in sql


class TestCopyFlat:
    def test_one_loaded_file(self):
        cursor = FakeCursor(COPY_COLUMNS, [loaded_row("s3://b/weather/flat/dt=2026-10-27/output_x.csv", 600, 600)])
        result = copy_flat(FakeConnection(cursor), "2026-10-27")
        assert result == LoadResult(files_loaded=1, rows_loaded=600, rows_parsed=600, errors_seen=0)

    def test_two_files_are_summed(self):
        cursor = FakeCursor(COPY_COLUMNS, [loaded_row("a.csv", 600, 600), loaded_row("b.csv", 600, 600)])
        result = copy_flat(FakeConnection(cursor), "2026-10-26")
        assert result.files_loaded == 2
        assert result.rows_loaded == 1200

    def test_nothing_new_is_a_status_row_without_rows_loaded(self):
        cursor = FakeCursor(("status",), [("Copy executed with 0 files processed.",)])
        result = copy_flat(FakeConnection(cursor), "2026-10-27")
        assert result == LoadResult(files_loaded=0, rows_loaded=0, rows_parsed=0, errors_seen=0)

    def test_sends_the_copy_sql_for_that_date(self):
        cursor = FakeCursor(("status",), [("Copy executed with 0 files processed.",)])
        copy_flat(FakeConnection(cursor), "2026-10-27")
        assert cursor.executed == [copy_sql("2026-10-27")]

    def test_programming_error_becomes_load_error_with_query_id(self):
        error = ProgrammingError(msg="Remote file not found", sfqid="01b0-fake-query-id")
        cursor = FakeCursor(error=error)
        with pytest.raises(LoadError) as excinfo:
            copy_flat(FakeConnection(cursor), "2026-10-27")
        message = str(excinfo.value)
        assert "01b0-fake-query-id" in message
        assert "dt=2026-10-27" in message
        assert "@RAW.S3_FLAT_STAGE/dt=2026-10-27/" in message
        assert isinstance(excinfo.value.__cause__, ProgrammingError)

    def test_bad_date_sends_no_sql(self):
        cursor = FakeCursor()
        with pytest.raises(ValueError):
            copy_flat(FakeConnection(cursor), "not-a-date")
        assert cursor.executed == []

    def test_cursor_is_closed_on_success_and_on_error(self):
        ok = FakeCursor(("status",), [("Copy executed with 0 files processed.",)])
        copy_flat(FakeConnection(ok), "2026-10-27")
        assert ok.closed

        failing = FakeCursor(error=ProgrammingError(msg="boom", sfqid="q"))
        with pytest.raises(LoadError):
            copy_flat(FakeConnection(failing), "2026-10-27")
        assert failing.closed
