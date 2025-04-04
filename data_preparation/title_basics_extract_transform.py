import pyspark.sql.types as t 
from pyspark.sql import functions as F


def filter_df_by_column_values(df, col_name, values, invert_filter=False):
    if invert_filter:
        condition = ~F.col(col_name).isin(values)
    else:
        condition = F.col(col_name).isin(values)
    
    return df.filter(condition)
    

def title_basics_extract_transform(spark, dataset_path):
    """
    Reads, cleans, and transforms the IMDb title.basics dataset.

    Args:
        spark (SparkSession): The active Spark session.
        dataset_path (str): The path to the IMDb title.basics.tsv.gz dataset.

    Returns:
        DataFrame: A cleaned and transformed Spark DataFrame with the following columns:
        
            - tconst (str): A tconst, an alphanumeric unique identifier of the title.
            - titleType (str): The type/format of the title (e.g., movie, short, etc.) 
            - primaryTitle (str): The most commonly used title or the title used 
              for promotional materials.
            - originalTitle (str): The original title in the original language.
            - isAdult (int): Indicates whether the title is for adults (1) or not (0).
            - startYear (date): The release year of the title. For TV series, this is 
              the series start year.
            - endYear (date): The end year for TV series, or None for other 
              title types.
            - runtimeMinutes (int): The primary runtime of the title in minutes.
            - genres (list of str): Up to three genres associated with the title.
    """

    schema = t.StructType([
        t.StructField('tconst', t.StringType(), True),
        t.StructField('titleType', t.StringType(), True),
        t.StructField('primaryTitle', t.StringType(), True),
        t.StructField('originalTitle', t.StringType(), True),
        t.StructField('isAdult', t.IntegerType(), True),
        t.StructField('startYear', t.StringType(), True), 
        t.StructField('endYear', t.StringType(), True),   
        t.StructField('runtimeMinutes', t.StringType(), True), 
        t.StructField('genres', t.StringType(), True),
    ])

    df = spark.read.csv(dataset_path, header=True, schema=schema, sep="\t")

    df = df.withColumn(
        "startYear",
        F.when(F.col("startYear") == "\\N", None)
        .otherwise(F.col("startYear").cast(t.IntegerType()))
    )

    df = df.withColumn(
        "endYear",
        F.when(F.col("endYear") == "\\N", None)
        .otherwise(F.col("endYear").cast(t.IntegerType()))
    )

    df = df.withColumn(
        "runtimeMinutes",
        F.when(F.col("runtimeMinutes") == "\\N", None)
        .otherwise(F.col("runtimeMinutes").cast(t.IntegerType()))
)

    values_to_filter = [0, 1]
    df = filter_df_by_column_values(df, "isAdult", values_to_filter)
    df = filter_df_by_column_values(df, "originalTitle", values_to_filter, invert_filter=True)
    df = filter_df_by_column_values(df, "primaryTitle", values_to_filter, invert_filter=True)
    
    df = df.filter(~(F.col("genres").isNull() | (F.col("genres") == "\\N")))


    return df






