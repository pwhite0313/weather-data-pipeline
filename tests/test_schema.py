import pandas as pd

from src.schema import FORECAST_COLUMNS, conform_to_schema


def _payload(**overrides) -> pd.DataFrame:
    """A one row frame carrying only the columns a caller names."""
    base = {"dt": [1778284800], "city_id": [5128581], "city_name": ["New York"]}
    base.update({k: [v] for k, v in overrides.items()})
    return pd.DataFrame(base)


class TestColumnContract:
    def test_output_has_exactly_the_canonical_columns(self):
        result = conform_to_schema(_payload())
        assert list(result.columns) == FORECAST_COLUMNS

    def test_column_order_is_stable_regardless_of_input_order(self):
        forward = conform_to_schema(pd.DataFrame({"dt": [1], "city_id": [2]}))
        reversed_ = conform_to_schema(pd.DataFrame({"city_id": [2], "dt": [1]}))
        assert list(forward.columns) == list(reversed_.columns)

    def test_snow_absent_in_summer_payload_is_filled_with_null(self):
        # The bug this module exists for: no snow anywhere means the API omits
        # snow_3h, which used to create a raw table with no such column.
        result = conform_to_schema(_payload(rain_3h=0.5))
        assert "snow_3h" in result.columns
        assert result["snow_3h"].isna().all()

    def test_rain_absent_in_dry_payload_is_filled_with_null(self):
        result = conform_to_schema(_payload(snow_3h=1.2))
        assert "rain_3h" in result.columns
        assert result["rain_3h"].isna().all()

    def test_unknown_column_is_dropped(self):
        result = conform_to_schema(_payload(some_new_api_field="x"))
        assert "some_new_api_field" not in result.columns

    def test_known_values_survive(self):
        result = conform_to_schema(_payload())
        assert result["city_name"].iloc[0] == "New York"
        assert result["city_id"].iloc[0] == 5128581


class TestDtypePinning:
    def test_integer_column_does_not_render_as_float(self):
        # "10000.0" is not valid input for a BIGINT column, which is how this
        # surfaced: one null in the payload promoted the column to float.
        result = conform_to_schema(_payload(visibility=10000.0))
        assert str(result["visibility"].iloc[0]) == "10000"

    def test_integer_column_tolerates_nulls(self):
        df = pd.DataFrame({"dt": [1, 2], "visibility": [10000.0, None]})
        result = conform_to_schema(df)
        assert str(result["visibility"].iloc[0]) == "10000"
        assert pd.isna(result["visibility"].iloc[1])

    def test_integer_dtype_is_stable_across_payloads(self):
        complete = conform_to_schema(pd.DataFrame({"visibility": [10000, 9000]}))
        partial = conform_to_schema(pd.DataFrame({"visibility": [10000, None]}))
        assert complete["visibility"].dtype == partial["visibility"].dtype

    def test_float_column_keeps_precision(self):
        result = conform_to_schema(_payload(city_coord_lat=40.7128))
        assert result["city_coord_lat"].iloc[0] == 40.7128

    def test_non_numeric_text_in_numeric_column_becomes_null(self):
        # A corrupted CSV row can put text where a number belongs. Coercing to
        # null keeps the load alive so the dbt not_null tests can reject it.
        result = conform_to_schema(_payload(visibility="01d"))
        assert pd.isna(result["visibility"].iloc[0])

    def test_text_columns_are_left_alone(self):
        result = conform_to_schema(_payload(weather_icon="01d"))
        assert result["weather_icon"].iloc[0] == "01d"
