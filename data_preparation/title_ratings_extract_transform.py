from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType


def title_ratings_extract_transform(spark: SparkSession, path: str):
    ratings_schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("averageRating", FloatType(), True),
        StructField("numVotes", IntegerType(), True)
    ])

    ratings_df = (
        spark.read.csv(path, header=True, schema=ratings_schema, sep="\t")
        .replace('\\N', None)
    )

    return ratings_df


if __name__ == '__main__':
    spark = SparkSession.builder.appName("IMDb Crew Data Processing").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    ratings_df = title_ratings_extract_transform(spark, path="../data/title.ratings.tsv")
    ratings_df.show()
