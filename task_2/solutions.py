from functools import lru_cache
from pathlib import Path

from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    FloatType,
)
from pyspark.sql import functions as F


@lru_cache(maxsize=1)
def get_spark():
    sc = SparkContext(master="local[1]", appName="ML Logs Transformer")
    spark = SparkSession(sc)
    return spark


def load_logs(logs_path: Path) -> DataFrame:
    schema = StructType(
            [
                StructField("logId", StringType()),
                StructField("expId", IntegerType()),
                StructField("metricId", IntegerType()),
                StructField("valid", BooleanType()),
                StructField("createdAt", StringType()),
                StructField("ingestedAt", StringType()),
                StructField("step", IntegerType()),
                StructField("value", FloatType()),
            ]
    )

    logs = get_spark().read.option("multiLine", "false").json(str(logs_path), schema=schema)
    return logs

def load_experiments(experiments_path: Path) -> DataFrame:
    schema = StructType(
            [StructField("expId", IntegerType()), StructField("expName", StringType())]
    )
    experiments = get_spark().read.option("header", "true").csv(str(experiments_path), schema=schema)
    return experiments

def load_metrics() -> DataFrame:
    arr = [(0, "Loss" ), (1, "Accuracy")]
    metrics = get_spark().createDataFrame(
        arr,
        StructType([
                StructField("metricId", IntegerType()),
                StructField("metricName", StringType()),
        ]),
    )
    return metrics    

def join_tables(
    logs: DataFrame, experiments: DataFrame, metrics: DataFrame
) -> DataFrame:

    joined_tables = logs \
        .join(experiments, experiments['expId'] == logs['expId'], how="inner") \
        .join(metrics, metrics['metricId'] == logs['metricId'], how="inner") \
        .select(
            logs["logId"],
            logs["expId"],
            logs["metricId"],
            logs["createdAt"],
            logs["ingestedAt"],
            logs["step"],
            logs["valid"],
            logs["value"],
            experiments["expName"],
            metrics["metricName"]
        )

    return joined_tables

def filter_late_logs(data: DataFrame, hours: int) -> DataFrame:
    data = data.withColumn("createdAt", F.to_timestamp("createdAt")) \
            .withColumn("ingestedAt", F.to_timestamp("ingestedAt"))

    filtered_logs = data.withColumn("time_diff", (F.unix_timestamp("ingestedAt") - F.unix_timestamp("createdAt")) / 3600)
    filtered_logs = filtered_logs.filter(F.col("time_diff") <= hours)
    filtered_logs.drop("time_diff")

    return filtered_logs

def calculate_experiment_final_scores(data: DataFrame) -> DataFrame:
    scores = data.groupBy("expId", "metricId", "expName", "metricName") \
        .agg(F.max("value").alias("maxValue"), F.min("value").alias("minValue"))
    return scores

def save(data: DataFrame, output_path: Path):
    data.write.partitionBy("metricId").mode("overwrite").parquet(str(output_path))
