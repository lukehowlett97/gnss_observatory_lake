import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Provide one local Spark session for unit tests."""

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gnss-observatory-lake-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
