import logging
import pandas as pd

from src.schema import conform_to_schema

logger = logging.getLogger(__name__)

def validate_response(data: list) -> None:
    if not isinstance(data, list) or len(data) == 0:
        raise ValueError("API response must be a non-empty list")

    for i, item in enumerate(data):
        if not isinstance(item, dict):
            raise ValueError(f"Response item {i} must be a dict")
        if "list" not in item or not isinstance(item["list"], list) or len(item["list"]) == 0:
            raise ValueError(f"Response item {i} missing required 'list' field")
        if "city" not in item or not isinstance(item["city"], dict):
            raise ValueError(f"Response item {i} missing required 'city' field")


def transform_records(data):

    logger.info("Transformation started")

    all_dfs = []

    try:
        validate_response(data)
    except ValueError:
        logger.exception("Validation failed")
        raise

    for response in data:
        # Normalize forecast records
        df = pd.json_normalize(response["list"], sep="_")

        # Normalize city metadata
        city_df = pd.json_normalize(response["city"], sep="_").add_prefix("city_")

        # Broadcast city metadata to all rows
        for col in city_df.columns:
            df[col] = city_df.iloc[0][col]

        all_dfs.append(df)

    df = pd.concat(all_dfs, ignore_index=True)

    # Extract first weather object from list (API returns weather as a list of dicts)
    # If missing or invalid, default to empty dict to avoid downstream errors
    df["weather"] = df["weather"].apply(
        lambda x: x[0] if isinstance(x, list) and len(x) > 0 else {}
    )

    df_weather = pd.json_normalize(df["weather"]).add_prefix("weather_")
    df = df.drop(columns=["weather"]).join(df_weather)

    # Convert column to date_time
    df["dt_txt"] = pd.to_datetime(df["dt_txt"], errors="coerce")
    df = df.dropna(subset=["dt_txt"])

    # Pin the column set so the CSV, and the raw table created from it, do not
    # vary with whichever optional fields the API happened to send today.
    df = conform_to_schema(df)

    logger.info("Transformed to Pandas DF of shape: %s", df.shape)

    # Append and return
    return df