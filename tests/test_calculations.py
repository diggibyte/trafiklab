"""Tests for PySpark helper calculations."""

import pytest
from pyspark.sql import functions as F

from htq2_gtfs.helpers.spark_helpers import (
    delay_minutes_col,
    haversine_col,
    is_first_stop_col,
    is_last_stop_col,
    punctuality_code_col,
    punctuality_col,
    speed_kph_col,
)


class TestPunctuality:
    """Test HTQ2 punctuality classification."""

    def test_early(self, spark):
        df = spark.createDataFrame([(-120,)], ["delay"])
        result = df.select(punctuality_col("delay").alias("p")).collect()[0]["p"]
        assert result == "Early"

    def test_on_time(self, spark):
        df = spark.createDataFrame([(30,)], ["delay"])
        result = df.select(punctuality_col("delay").alias("p")).collect()[0]["p"]
        assert result == "OnTime"

    def test_slightly_late(self, spark):
        df = spark.createDataFrame([(240,)], ["delay"])
        result = df.select(punctuality_col("delay").alias("p")).collect()[0]["p"]
        assert result == "SlightlyLate"

    def test_late(self, spark):
        df = spark.createDataFrame([(500,)], ["delay"])
        result = df.select(punctuality_col("delay").alias("p")).collect()[0]["p"]
        assert result == "Late"

    def test_null_delay(self, spark):
        df = spark.createDataFrame([(None,)], ["delay"])
        result = df.select(punctuality_col("delay").alias("p")).collect()[0]["p"]
        assert result is None

    def test_code_on_time(self, spark):
        df = spark.createDataFrame([(0,)], ["delay"])
        result = df.select(punctuality_code_col("delay").alias("c")).collect()[0]["c"]
        assert result == 2  # OnTime

    def test_boundary_early(self, spark):
        """Exactly -60s should be OnTime (threshold is < -60)."""
        df = spark.createDataFrame([(-60,)], ["delay"])
        result = df.select(punctuality_col("delay").alias("p")).collect()[0]["p"]
        assert result == "OnTime"


class TestDelay:
    """Test delay calculations."""

    def test_delay_minutes(self, spark):
        df = spark.createDataFrame([(180,)], ["sec"])
        result = df.select(delay_minutes_col("sec").alias("m")).collect()[0]["m"]
        assert result == 3.0

    def test_delay_minutes_negative(self, spark):
        df = spark.createDataFrame([(-90,)], ["sec"])
        result = df.select(delay_minutes_col("sec").alias("m")).collect()[0]["m"]
        assert result == -1.5


class TestHaversine:
    """Test haversine distance calculation."""

    def test_same_point(self, spark):
        df = spark.createDataFrame([(55.6, 13.0, 55.6, 13.0)], ["lat1", "lon1", "lat2", "lon2"])
        result = df.select(haversine_col("lat1", "lon1", "lat2", "lon2").alias("d")).collect()[0]["d"]
        assert abs(result) < 0.01  # Should be ~0

    def test_known_distance(self, spark):
        """Malmö Central to Lund Central is ~18km."""
        df = spark.createDataFrame(
            [(55.6093, 13.0003, 55.7058, 13.1872)],
            ["lat1", "lon1", "lat2", "lon2"],
        )
        result = df.select(haversine_col("lat1", "lon1", "lat2", "lon2").alias("d")).collect()[0]["d"]
        assert 15000 < result < 20000  # ~18km


class TestSpeed:
    """Test speed calculations."""

    def test_normal_speed(self, spark):
        df = spark.createDataFrame([(1000, 120)], ["dist", "dur"])
        result = df.select(speed_kph_col("dist", "dur").alias("s")).collect()[0]["s"]
        assert result == 30.0  # 1km in 120s = 30 km/h

    def test_zero_duration(self, spark):
        df = spark.createDataFrame([(1000, 0)], ["dist", "dur"])
        result = df.select(speed_kph_col("dist", "dur").alias("s")).collect()[0]["s"]
        assert result is None


class TestSequenceHelpers:
    """Test first/last stop detection."""

    def test_first_stop(self, spark):
        df = spark.createDataFrame([(1,)], ["seq"])
        result = df.select(is_first_stop_col("seq").alias("f")).collect()[0]["f"]
        assert result == "Yes"

    def test_not_first_stop(self, spark):
        df = spark.createDataFrame([(5,)], ["seq"])
        result = df.select(is_first_stop_col("seq").alias("f")).collect()[0]["f"]
        assert result == "No"

    def test_last_stop(self, spark):
        df = spark.createDataFrame([(10, 10)], ["seq", "last"])
        result = df.select(is_last_stop_col("seq", "last").alias("l")).collect()[0]["l"]
        assert result == "Yes"
