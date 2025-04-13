from pyspark.sql import SparkSession

from data_preparation.title_basics_extract_transform import title_basics_extract_transform
from data_preparation.title_ratings_extract_transform import title_ratings_extract_transform
from data_preparation.title_akas_extract_transform import title_akas_extract_transform

from business_queries.query_frenis import top_comedy_movies_after_2010


def main():
    spark = SparkSession.builder.appName("IMDB Data Analysis").getOrCreate()

    title_basics_data_path = "./title.basics.tsv"
    title_basics_data_path = "./title.ratings.tsv"
    title_basics_data_path = "./title.akas.tsv"
    
    title_basics = title_basics_extract_transform(spark, title_basics_data_path)
    title_ratings = title_ratings_extract_transform(spark, title_basics_data_path)
    title_akas =  title_akas_extract_transform(spark, title_basics_data_path)

    top_5_comedy_movies = top_comedy_movies_after_2010(title_basics, title_ratings, title_akas)
    print(top_5_comedy_movies.show())

if __name__ == "__main__":
    main()


