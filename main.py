from business_queries.query_frenis import top_comedy_movies_after_2010
from data_preparation.title_basics_extract_transform import title_basics_extract_transform
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("IMDB Data Analysis").getOrCreate()

    title_basics_data_path = "./title.basics.tsv"
    title_basics = title_basics_extract_transform(spark, title_basics_data_path)
    
    title_ratings = spark.read.csv(
        "./title.ratings.cleaned.tsv", sep="\t", header=True, inferSchema=True
    )
    title_akas = spark.read.csv(
        "./title.akas.cleaned.tsv", sep="\t", header=True, inferSchema=True
    )

    top_5_comedy_movies = top_comedy_movies_after_2010(title_basics, title_ratings, title_akas)
    print(top_5_comedy_movies.show())

if __name__ == "__main__":
    main()