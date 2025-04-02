from data_preparation.title_akas_extract_transform import title_akas_extract_transform
from business_queries.query_ufimtseva import compute_language_rating_trends
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName(
        "IMDb Dataset Processing").getOrCreate()
    # dataset_path = "data/title.akas.tsv"
    #
    # df = title_akas_extract_transform(spark, dataset_path)
    # df.printSchema()

    title_basics = spark.read.csv("data/title_basics_cleaned.csv", sep="\t", #todo rename to tsv
                                  header=True, inferSchema=True)
    title_ratings = spark.read.csv("data/title.ratings.cleaned.tsv", sep="\t",
                                   header=True, inferSchema=True)
    title_akas = spark.read.csv("data/title.akas.cleaned.tsv", sep="\t", header=True,
                                inferSchema=True)

    df = compute_language_rating_trends(spark, title_basics, title_ratings, title_akas)
    df.show()


if __name__ == '__main__':
    main()
    