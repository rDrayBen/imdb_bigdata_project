from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructType, StructField

def name_basics_extract_transform_optimized(data_path: str):
    spark = SparkSession.builder.getOrCreate()

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
        .replace("\\N", None)  # Replace "\N" with null values
        .drop("birthYear", "deathYear")  # Drop unneeded columns
        .dropna(subset=["primaryName"])  # Remove rows with null primaryName
        .filter(col("primaryProfession").isNotNull() | col("knownForTitles").isNotNull())  # Remove rows where both are null
        .na.fill("unknown")  # Replace remaining nulls with "unknown"
    )

    return df


if __name__ == '__main__':
    name_basics_extract_transform_optimized('/app/data/name.basics.tsv')
