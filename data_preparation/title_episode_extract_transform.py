from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from pyspark.sql import functions as F


def title_episode_extract_transform(spark, data_path: str):
    schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("parentTconst", StringType(), True),
        StructField("seasonNumber", IntegerType(), True),
        StructField("episodeNumber", IntegerType(), True)
    ])

    df = spark.read.csv(data_path, sep="\t", header=True, schema=schema)
    df = df.replace("\\N", None)
    df = df.dropna()
    df = df.filter(~((F.col("seasonNumber") > 259) | (F.col("episodeNumber") > 15762)))

    return df


