from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col


def title_crew_extract_transform(spark: SparkSession, path: str):
    crew_schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("directors", StringType(), True),
        StructField("writers", StringType(), True)
    ])

    crew_df = (
        spark.read.csv(path, header=True, schema=crew_schema, sep="\t")
        .replace('\\N', None)
        .filter(~(col("directors").isNull() & col("writers").isNull()))
    )

    return crew_df


if __name__ == '__main__':
    spark = SparkSession.builder.appName("IMDb Crew Data Processing").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    crew_df = title_crew_extract_transform(spark, path="../data/title.crew.tsv")
    crew_df.show()
