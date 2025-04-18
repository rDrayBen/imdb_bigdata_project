from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pycountry
from pyspark.sql.functions import udf, col, sum


def title_akas_extract_transform(spark, dataset_path):
    """
     Reads, cleans, and transforms the IMDb title.akas dataset.
     Args:
         spark (SparkSession): The active Spark session.
         dataset_path (str): The path to the IMDb title.akas.tsv dataset.

     Returns:
         DataFrame: A cleaned and transformed Spark DataFrame with the following columns:
             - titleId (str): A tconst, an alphanumeric unique identifier of the title.
             - ordering (int): A number to uniquely identify rows for a given titleId.
             - title (str): The localized title.
             - region (str): The region for this version of the title or "Unknown".
             - language (str): The language of the title or "Unknown".
             - isOriginalTitle (int): Indicates whether it is the original title (1) or not (0).
             - country (str): Full country name (derived from the region).
     """
    def get_country_name(region_code):
        try:
            country = pycountry.countries.get(alpha_2=region_code)
            if country:
                return country.name
            else:
                return region_code
        except Exception as e:
            return region_code

    schema = StructType([
        StructField("titleId", StringType(), True),
        StructField("ordering", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("region", StringType(), True),
        StructField("language", StringType(), True),
        StructField("types", StringType(), True),
        StructField("attributes", StringType(), True),
        StructField("isOriginalTitle", IntegerType(), True),
    ])

    df = spark.read.csv(dataset_path, header=True, schema=schema, sep="\t")
    df = df.drop("attributes", "types")
    df = df.dropna(subset=["isOriginalTitle"])

    df = df.fillna({"region": "Unknown", "language": "Unknown"})
    df = df.replace("\\N", "Unknown", subset=["region", "language"])

    get_country_name_udf = udf(get_country_name, StringType())

    df = df.withColumn("country",
                                      get_country_name_udf(df["region"]))

    enforced_schema = StructType([
        StructField("titleId", StringType(), False),
        StructField("ordering", IntegerType(), False),
        StructField("title", StringType(), False),
        StructField("region", StringType(), False),
        StructField("language", StringType(), False),
        StructField("isOriginalTitle", IntegerType(), False),
        StructField("country", StringType(), False)
    ])

    df = spark.createDataFrame(df.rdd, schema=enforced_schema)
    return df

