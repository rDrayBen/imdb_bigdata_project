from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from pyspark.sql import functions as F


def title_episode_extract_and_transform(data_path: str):
    spark = SparkSession.builder.getOrCreate()

    schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("parentTconst", StringType(), True),
        StructField("seasonNumber", IntegerType(), True),
        StructField("episodeNumber", IntegerType(), True)
    ])

    # Read the data with the defined schema
    df = spark.read.csv(data_path, sep="\t", header=True, schema=schema)

    # Replace "\N" values with None
    df = df.replace("\\N", None)

    # Drop null values
    df = df.dropna()

    # Filter invalid data
    df = df.filter(~((F.col("seasonNumber") > 259) | (F.col("episodeNumber") > 15762)))
    return df


