from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import IntegerType, StringType, StructType, StructField


def title_principals_extract_and_transform(spark, data_path: str):
    schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("ordering", IntegerType(), True),
        StructField("nconst", StringType(), True),
        StructField("category", StringType(), True),
        StructField("job", StringType(), True),
        StructField("characters", StringType(), True)
    ])
    df = spark.read.csv(data_path, sep="\t", header=True, schema=schema)
    df = df.withColumn("characters", when(col("characters") == "\\N", "unknown").otherwise(col("characters")))
    df = df.drop('job')

    return df