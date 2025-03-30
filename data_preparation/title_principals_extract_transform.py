from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import IntegerType, StringType, StructType, StructField


def title_principals_extract_and_transform(data_path: str):
    spark = SparkSession.builder.getOrCreate()

    # Define schema explicitly
    schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("ordering", IntegerType(), True),
        StructField("nconst", StringType(), True),
        StructField("category", StringType(), True),
        StructField("job", StringType(), True),
        StructField("characters", StringType(), True)
    ])

    # Read the data with the defined schema
    df = spark.read.csv(data_path, sep="\t", header=True, schema=schema)

    # Replace '\N' in 'characters' column with 'unknown'
    df = df.withColumn("characters", when(col("characters") == "\\N", "unknown").otherwise(col("characters")))

    # Drop the 'job' column
    df = df.drop('job')

    return df