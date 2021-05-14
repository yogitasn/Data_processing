import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DecimalType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql import functions as F
from pyspark.sql.functions import col, array_contains, date_format, regexp_replace
import logging
import configparser
from pathlib import Path
from datetime import datetime, timedelta
from pyspark.sql.functions import udf
import re
import os
import re
import time
import shutil

from dataset_processing.utilities.miscProcess import GenerateLogs

SCRIPT_NAME = os.path.basename(__file__)


class OccupancyProcessing:
    """
    This class works on processing historic and delta datasets for Seattle Paid Parking Occupancy

    """

    def __init__(self, spark):
        self.spark = spark

    def global_SQLContext(self, spark1):
        global spark
        self.spark = spark1

    def global_EffectDt(self, iEffevtDt):
        global EffectvDt
        EffectvDt = datetime.strptime(iEffevtDt, "%Y-%m%d").date()

    def get_currentDate(self):
        current_time = datetime.now()
        str_current_time = current_time.strftime("%Y-%m-%d")
        gen_logs = GenerateLogs(self.spark)
        gen_logs.log_print(str_current_time)

    def regex_replace_values():
        regexp_replace

    """ Function to remove non word characters e.g. '19,788' -> '19788'"""

    def remove_non_word_characters(self, col):
        return F.regexp_replace(col, "[^\\w\\s]+", "")

    """ Function to remove parenthesis e.g. (108.88 > 108.88 """

    def remove__parenthesis(self, col):
        return F.regexp_replace(col, "\(|\)", "")

    """ Function to convert timestamp column value to a specific date format """

    def date__format(self, col, formattype):
        return F.date_format(col, formattype)

    """ Function to convert a column to a timestampformat """

    def timestamp_format(self, col, timestampformat):
        return F.to_timestamp(col, format=timestampformat)

    """ Read Parquet function - input file path, schema and partition name """

    def sourceOccupancyReadParquet(self, occupancyFilePath, custom_schema, partition_value):
        gen_logs = GenerateLogs(self.spark)
        gen_logs.log_info(SCRIPT_NAME, "Reading Occupancy CSV file...")
        print("Reading Occupancy CSV file")

        source_data_info = {}
        source_data_info["type"] = "CSV"

        # filepath = source_config['sources']['driverSource']["filePath"]
        print("Occupancy file path : {}".format(occupancyFilePath))

        try:
            occupancy = (
                self.spark.read.format("csv").option("header", True).schema(custom_schema).load(occupancyFilePath)
            )

        except Exception as e:
            gen_logs.log_info(SCRIPT_NAME, "error in reading csv: {}".format(e))

        source_data_info["occupancyFilePath"] = occupancyFilePath
        source_data_info["partition"] = str(partition_value)

        occupancy.show(3)

        return (occupancy, source_data_info)

    """ Function to create a dataframe used in a broadcast while joining two dataframes """

    def createStationIDDF(self, path_of_year_tob_processed, cust_schema):

        station_id_df = (
            self.spark.read.format("csv").option("header", True).schema(cust_schema).load(path_of_year_tob_processed)
        )

        station_id_df = station_id_df.withColumn("station_id", F.col("sourceelementkey"))
        station_id_df = station_id_df.drop("sourceelementkey")

        station_id_df = station_id_df.withColumn("station_id", self.remove_non_word_characters(F.col("station_id")))

        station_id_df = station_id_df.withColumn("longitude", F.split("location", " ").getItem(1)).withColumn(
            "latitude", F.split("location", " ").getItem(2)
        )

        station_id_df = station_id_df.withColumn("latitude", self.remove__parenthesis(F.col("latitude"))).withColumn(
            "longitude", self.remove__parenthesis(F.col("longitude"))
        )

        station_id_df = station_id_df.withColumn("latitude", station_id_df["latitude"].cast(DoubleType())).withColumn(
            "longitude", station_id_df["longitude"].cast(DoubleType())
        )

        station_id_df = station_id_df.drop("location")

        # get the distinct Station_Id, Longitude and Latitude
        station_id_lookup = station_id_df.select("station_Id", "longitude", "latitude").distinct()

        station_id_lookup.persist()

        # Broadcast the smaller dataframe as it contains few 1000 rows
        F.broadcast(station_id_lookup)

        station_id_lookup.show(2)

        return station_id_lookup

    """ Function to transform historic and delta paid parking occupancy datasets (>=2018) """

    def executeOccupancyOperations(
        self, src_df, output, datedimoutputpath, cols_list, partn_col, max_retry_count, retry_delay
    ):

        PartitionColumn = partn_col
        gen_logs = GenerateLogs(self.spark)

        ReturnCode = 0
        rec_cnt = 0
        RetryCt = 0
        Success = False

        while (RetryCt < max_retry_count) and not Success:

            try:
                Success = True
                # reading from DBFS
                input_df = src_df

            except:
                Success = False
                RetryCt += 1
                if RetryCt == max_retry_count:
                    gen_logs.log_info(
                        SCRIPT_NAME, "Failed on reading input file after {} tries: {}".format(max_retry_count)
                    )
                    ReturnCode = 1
                    return ReturnCode, rec_cnt

                else:
                    gen_logs.log_info(
                        SCRIPT_NAME, "Failed on reading input file, re-try in {} seconds ".format(retry_delay)
                    )

        select_df = input_df.select([colname for colname in input_df.columns])
        # if colname in (cols_list)])

        select_df = select_df.withColumn("station_id", F.col("sourceelementkey"))
        select_df = select_df.drop("sourceelementkey")

        print("Reading inside transformation function")
        select_df.show(5)

        for x in range(len(cols_list)):
            if cols_list[x] == "station_id":
                column = cols_list[x]

                select_df = select_df.withColumn(column, self.remove_non_word_characters(F.col("station_id")))
                select_df = select_df.withColumn(column, select_df[column].cast(IntegerType()))

            if cols_list[x] == "occupancydatetime":
                column = cols_list[x]

                self.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
                select_df = select_df.withColumn(column, self.timestamp_format(F.col(column), "MM/dd/yyyy hh:mm:ss a"))

                select_df = select_df.withColumn(PartitionColumn, self.date__format(F.col(column), "MMMM"))

                date_dim = select_df.withColumn("day_of_week", self.date__format(F.col(column), "EEEE")).withColumn(
                    "month", self.date__format(F.col(column), "MMMM")
                )

                date_dim = date_dim.select("occupancydatetime", "day_of_week", "month")

                select_df = select_df.withColumn(PartitionColumn, self.date__format(F.col(column), "MMMM"))

            if cols_list[x] == "location":

                column = cols_list[x]
                split_col = ["longitude", "latitude"]

                select_df = select_df.withColumn(split_col[0], F.split(column, " ").getItem(1)).withColumn(
                    split_col[1], F.split(column, " ").getItem(2)
                )

                select_df = select_df.withColumn(split_col[0], self.remove__parenthesis(col(split_col[0]))).withColumn(
                    split_col[1], self.remove__parenthesis(col(split_col[1]))
                )

                select_df = select_df.withColumn(split_col[0], select_df[split_col[0]].cast(DoubleType())).withColumn(
                    split_col[1], select_df[split_col[1]].cast(DoubleType())
                )

                select_df = select_df.drop(column)

            #   select_df = select_df.select(cols_list)
            # select_df = select_df.select([colname for colname in input_df.columns if colname in (cols_list)])

        RetryCt = 0
        Success = False

        while (RetryCt < max_retry_count) and not Success:
            try:
                Success = True
                select_df.show(3)
                gen_logs.log_print("Writing occupancy dataframe to output file: {}".format(output))

                select_df.write.mode("append").partitionBy(PartitionColumn).parquet(output)

                gen_logs.log_print("Writing date dimension to output file: {}".format(datedimoutputpath))
                date_dim.show(3)
                # date_dim.write.mode("append").partitionBy(PartitionColumn).parquet(datedimoutputpath)
            except:
                Success = False
                RetryCt += 1
                if RetryCt == max_retry_count:
                    gen_logs.log_info(
                        SCRIPT_NAME,
                        "Failed on writing to Output after {} tries: {} ".format(max_retry_count, output),
                    )
                    ReturnCode = 2
                    return ReturnCode, rec_cnt
                else:
                    gen_logs.log_info(
                        SCRIPT_NAME, "Failed on writing to Output, re-try in {} seconds ".format(retry_delay)
                    )
                    time.sleep(retry_delay)

        gen_logs.log_print("Number of Records Processed: {}".format(rec_cnt))
        return ReturnCode, rec_cnt

    """ Function to transform historic paid parking datasets from 2012-2017 """

    def executeHistoricOccupancyOperations(
        self, src_df, output, path_of_year_tob_processed, partn_col, max_retry_count, retry_delay, custom_schema
    ):
        gen_logs = GenerateLogs(self.spark)

        PartitionColumn = partn_col
        station_id_lookup = self.createStationIDDF(path_of_year_tob_processed, custom_schema)

        src_df = src_df.withColumn("station_id", F.col("sourceelementkey"))
        src_df = src_df.drop("sourceelementkey")

        occ_df = src_df.join(station_id_lookup, ["station_id"], how="left_outer").select(
            src_df.occupancydatetime,
            src_df.station_id,
            src_df.paidoccupancy,
            src_df.available_spots,
            station_id_lookup.longitude,
            station_id_lookup.latitude,
        )

        self.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

        occ_df = occ_df.withColumn(
            "occupancydatetime", self.timestamp_format(F.col("occupancydatetime"), "MM/dd/yyyy hh:mm:ss a")
        )

        occ_df = occ_df.withColumn(PartitionColumn, self.date__format(F.col("occupancydatetime"), "MMMM"))

        ReturnCode = 0
        rec_cnt = 0
        RetryCt = 0
        Success = False

        while (RetryCt < max_retry_count) and not Success:
            try:
                Success = True
                occ_df.write.mode("append").partitionBy(PartitionColumn).parquet(output)
            except:
                Success = False
                RetryCt += 1
                if RetryCt == max_retry_count:
                    gen_logs.log_info(
                        SCRIPT_NAME, "Failed on writing to Output after {} tries: {} ".format(max_retry_count, output)
                    )
                    ReturnCode = 2
                    return ReturnCode, rec_cnt
                else:
                    gen_logs.log_info(
                        SCRIPT_NAME, "Failed on writing to Output, re-try in {} seconds ".format(retry_delay)
                    )
                    time.sleep(retry_delay)

        gen_logs.log_print("Number of Records Processed: {}".format(rec_cnt))
        return ReturnCode, rec_cnt

    def main(self):

        #    logger = spark._jvm.org.apache.log4j
        #   logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
        #  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

        self.executeOccupancyOperations(src_df, output, cols_list, cols_dict, partn_col, max_retry_count, retry_delay)


if __name__ == "__main__":
    log_file = "test.log"
    #  gen_logs = GenerateLogs(self.spark)
    #  GenerateLogs.initial_log_file(logfile)
    OccupancyProcessing.main()
