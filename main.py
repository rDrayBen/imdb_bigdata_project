from pyspark.sql import SparkSession

from data_preparation.name_basics_extract_transform import name_basics_extract_transform
from data_preparation.title_akas_extract_transform import title_akas_extract_transform
from data_preparation import title_principals_extract_transform

from business_queries import query_rabotiahov
from business_queries.query_ratushniak import count_actors_in_low_rated_popular_films
from business_queries.query_slobodian import high_rated_films_associated_actors
from business_queries.query_ufimtseva import compute_language_rating_trends


def main():
    spark = SparkSession.builder \
        .appName("IMDB Data Analysis") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    print("Driver Memory:", spark.sparkContext._conf.get("spark.driver.memory"))
    
    # Load and transform data
    name_basics_data_path = "./app/data/name.basics.tsv"
    title_akas_path = "./app/data/title.akas.tsv"
    title_ratings_path = "./app/data/title.ratings.tsv"
    title_principals_path = "./app/data/title.principals.tsv"
    name_basics_path = "./app/data/name.basics.tsv"
    title_principals_data_path = "data/title.principals.tsv"
   
    title_principals_df = title_principals_extract_transform(spark, title_principals_data_path)
    
    name_basics_df = name_basics_extract_transform(spark, name_basics_data_path)
    title_akas_df = title_akas_extract_transform(spark, title_akas_path)
    query_rabotiahov(title_principals_df)

    # Perform business queries
    actors_in_low_rated_popular_films_df = count_actors_in_low_rated_popular_films(
        title_ratings_df, 
        title_principals_df,
        name_basics_df, 
        average_rating=5.0, 
        top_n=5000
    )
    print("Count actors in low rated popular films")
    print(f"Total rows in resulting dataframe: {actors_in_low_rated_popular_films_df.count()}")
    actors_in_low_rated_popular_films_df.show(truncate=False, n=20)
    
    high_rated_films_associated_actors_df = high_rated_films_associated_actors(
        title_akas_df, 
        title_ratings_df, 
        title_principals_df, 
        name_basics_df, 
        top_n=100
    )
    print("High rated films associated actors")
    print(f"Total rows in resulting dataframe: {high_rated_films_associated_actors_df.count()}")
    high_rated_films_associated_actors_df.show(truncate=False, n=20)
   
    language_rating_trends_df = compute_language_rating_trends(
        title_basics, 
        title_ratings, 
        title_akas_df
    )
    print("Compute language rating trends")
    print(f"Total rows in resulting dataframe: {language_rating_trends_df.count()}")
    language_rating_trends_df.show(truncate=False, n=20)
