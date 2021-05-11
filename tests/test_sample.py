import findspark

findspark.init()

findspark.find()

import pyspark
import logging
import sys
from operator import add
import pytest
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

from pyspark.sql.types import ArrayType, DoubleType, BooleanType, StringType
from chispa.column_comparer import assert_column_equality
from pathlib import Path
import json
import os
from pyspark.sql import functions as F
from dataset_processing.blockface_processing.executeBlockface import BlockfaceProcessing
from dataset_processing.utilities.processDataframeConfig import DataframeConfig
from dataset_processing.utilities.miscProcess import GenerateLogs
from dataset_processing.occupancy_processing.executeOccupancyProcess import OccupancyProcessing


SCRIPT_NAME = os.path.basename(__file__)

path = str(Path(Path(__file__).parent.absolute()).parent.absolute())
logging.info(path)


def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session(request):
    """Fixture for creating a spark context."""

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("pytest-pyspark-local-testing")
        .enableHiveSupport()
        .getOrCreate()
    )
    request.addfinalizer(lambda: spark.stop())

    quiet_py4j()
    return spark


pytestmark = pytest.mark.usefixtures("spark_session")


def test_remove_non_word_characters(spark_session):

    occ_pr = OccupancyProcessing(spark_session)

    data = [("jo&&se", "jose"), ("**li**", "li"), ("77,990", "77990"), (None, None)]
    df = spark_session.createDataFrame(data, ["name", "expected_name"]).withColumn(
        "clean_name", occ_pr.remove_non_word_characters(F.col("name"))
    )
    assert_column_equality(df, "clean_name", "expected_name")


pytestmark = pytest.mark.usefixtures("spark_session")


def test_column_list(spark_session):
    GenerateLogs.global_SQLContext(spark_session)
    GenerateLogs.initial_log_file("test_log.log")

    json_file = "C://Datasetprocessing//tests//test1.json"

    with open(json_file) as jfile:
        df_dict = json.load(jfile)

    actual_list = DataframeConfig.build_dataframe_column_list(df_dict)

    expected_list = ["occupancydatetime", "occupied_spots"]
    print(actual_list)

    assert actual_list[0] == expected_list[0]
    assert actual_list[1] == expected_list[1]


pytestmark = pytest.mark.usefixtures("spark_session")


def test_file_path(spark_session):
    GenerateLogs.global_SQLContext(spark_session)
    GenerateLogs.initial_log_file("test_log.log")

    json_file = "C://Datasetprocessing//tests//test1.json"

    with open(json_file) as jfile:
        df_dict = json.load(jfile)

    actual_filepath = DataframeConfig.get_source_driverFilerPath(df_dict)

    expected_filePath = "C:\\Test\\Paid_Parking.csv"

    assert actual_filepath == expected_filePath


pytestmark = pytest.mark.usefixtures("spark_session")


def test_dataframe_partition(spark_session):

    GenerateLogs.global_SQLContext(spark_session)
    GenerateLogs.initial_log_file("test_log.log")

    json_file = "C://Datasetprocessing//tests//test1.json"

    with open(json_file) as jfile:
        df_dict = json.load(jfile)

    actual_partition = DataframeConfig.partition_column(df_dict)

    expected_partition = "MONTH"

    assert actual_partition == expected_partition


pytestmark = pytest.mark.usefixtures("spark_session")


def test_remove_parenthesis_characters(spark_session):

    occ_pr = OccupancyProcessing(spark_session)

    data = [("(123.88", "123.88"), ("6788.9)", "6788.9")]
    df = spark_session.createDataFrame(data, ["name", "expected_name"]).withColumn(
        "clean_name", occ_pr.remove__parenthesis((F.col("name")))
    )

    actual_data_list = df.select("clean_name").collect()

    actual_data_array = [float(row["clean_name"]) for row in actual_data_list]

    expected_data_list = df.select("expected_name").collect()

    expected_data_array = [float(row["expected_name"]) for row in expected_data_list]

    assert actual_data_array[0] == expected_data_array[0]
    assert actual_data_array[1] == expected_data_array[1]


pytestmark = pytest.mark.usefixtures("spark_session")


def test_date_format(spark_session):
    spark_session.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    occ_pr = OccupancyProcessing(spark_session)

    data = [
        ("04/15/2021 02:33:00 PM", "April"),
        ("05/17/2021 08:01:00 AM", "May"),
    ]
    df = spark_session.createDataFrame(data, ["Datetime", "expected_date_fmt_value"])

    df = df.withColumn("Datetime", occ_pr.timestamp_format(F.col("Datetime"), "MM/dd/yyyy hh:mm:ss a"))

    df = df.withColumn("actual_month", occ_pr.date__format(F.col("Datetime"), "MMMM"))

    actual_month_data_list = df.select("actual_month").collect()

    actual_month_data_array = [str(row["actual_month"]) for row in actual_month_data_list]

    expected_month_data_list = df.select("expected_date_fmt_value").collect()

    expected_month_data_array = [str(row["expected_date_fmt_value"]) for row in expected_month_data_list]

    assert actual_month_data_array[0] == expected_month_data_array[0]
    assert actual_month_data_array[1] == expected_month_data_array[1]


pytestmark = pytest.mark.usefixtures("spark_session")


def test_read_blockface(spark_session):
    spark_session.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    blockface_pr = BlockfaceProcessing(spark_session)

    blockface_config_dict = DataframeConfig.json_reader(
        "C:\\Datasetprocessing\\dataset_processing\\data\\blockface.json"
    )

    blockfacefilePath = DataframeConfig.get_source_driverFilerPath(blockface_config_dict)

    # Get Target Table Schema
    TargetDataframeSchema = DataframeConfig.get_dataframe_schema(blockface_config_dict)

    (blockface, source_data_info) = blockface_pr.sourceBlockfaceReadParquet(blockfacefilePath, TargetDataframeSchema)

    blockface.printSchema()

    blockface.head(4)


def test_read_occupancy(spark_session):
    spark_session.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    occ_pr = OccupancyProcessing(spark_session)

    occ_config_dict = DataframeConfig.json_reader("C:\\Datasetprocessing\\dataset_processing\\data\\occupancy.json")

    occfilePath = DataframeConfig.get_source_driverFilerPath(occ_config_dict)

    # Get Target Table Schema
    TargetDataframeSchema = DataframeConfig.get_dataframe_schema(occ_config_dict)

    (occupancy, source_data_info) = occ_pr.sourceOccupancyReadParquet(occfilePath, TargetDataframeSchema, "MONTH")

    occupancy.printSchema()

    occupancy.head(4)
