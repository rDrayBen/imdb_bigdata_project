from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructType, StructField

def name_basics_extract_transform(spark, data_path: str):
    schema = StructType([
        StructField("nconst", StringType(), True),
        StructField("primaryName", StringType(), True),
        StructField("birthYear", StringType(), True),
        StructField("deathYear", StringType(), True),
        StructField("primaryProfession", StringType(), True),
        StructField("knownForTitles", StringType(), True)
    ])

    df = (
        spark.read.csv(data_path, sep="\t", header=True, schema=schema)
        .replace("\\N", None)
        .drop("birthYear", "deathYear")
        .dropna(subset=["primaryName"]) 
        .filter(col("primaryProfession").isNotNull() | col("knownForTitles").isNotNull())
        .na.fill("unknown")
    )

    return df


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate() 
    name_basics_extract_transform(spark, '/app/data/name.basics.tsv')
