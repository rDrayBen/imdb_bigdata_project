from pyspark.sql import SparkSession

from business_queries.query_ufimtseva import compute_language_rating_trends
from data_preparation.title_akas_extract_transform import \
    title_akas_extract_transform


def main():
    spark = SparkSession.builder.appName("IMDB Data Analysis").getOrCreate()
    dataset_path = "data/title.akas.tsv"

    title_akas_df = title_akas_extract_transform(spark, dataset_path)

    title_basics = spark.read.csv("data/title_basics_cleaned.csv", sep="\t", #todo rename to tsv
                                  header=True, inferSchema=True)
    title_ratings = spark.read.csv("data/title.ratings.cleaned.tsv", sep="\t",
                                   header=True, inferSchema=True)

    df = compute_language_rating_trends(title_basics, title_ratings, title_akas_df)
    df.show()
if __name__ == '__main__':
  main()
