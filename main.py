from pyspark.sql import SparkSession

from data_preparation.name_basics_extract_transform import name_basics_extract_transform
from data_preparation.title_akas_extract_transform import title_akas_extract_transform
from data_preparation.title_basics_extract_transform import title_basics_extract_transform
from data_preparation.title_crew_extract_transform import title_crew_extract_transform
from data_preparation.title_episode_extract_transform import title_episode_extract_transform
from data_preparation.title_principals_extract_transform import title_principals_extract_transform
from data_preparation.title_ratings_extract_transform import title_ratings_extract_transform

from business_queries.query_dolynska import query_dolynska
from business_queries.query_frenis import top_comedy_movies_after_2010
from business_queries.query_rabotiahov import top_genres_average_rating_over_decades
from business_queries.query_ratushniak import count_actors_in_low_rated_popular_films
from business_queries.query_slobodian import high_rated_films_associated_actors
from business_queries.query_ufimtseva import compute_language_rating_trends


def main():
    spark = SparkSession.builder \
        .appName("IMDB Data Analysis") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    print("Driver Memory:", spark.sparkContext._conf.get("spark.driver.memory"))
    
    # Load and transform data
    name_basics_path = "./app/data/name.basics.tsv"
    title_akas_path = "./app/data/title.akas.tsv"
    title_basics_path = "./app/data/title.basics.tsv"
    title_crew_path = "./app/data/title.crew.tsv"
    title_episode_path = "./app/data/title.episode.tsv"
    title_principals_path = "./app/data/title.principals.tsv"
    title_ratings_path = "./app/data/title.ratings.tsv"

    name_basics_df = name_basics_extract_transform(spark, name_basics_path)
    title_akas_df = title_akas_extract_transform(spark, title_akas_path)
    title_basics_df = title_basics_extract_transform(spark, title_basics_path)
    title_crew_df = title_crew_extract_transform(spark, title_crew_path)
    title_episode_df = title_episode_extract_transform(spark, title_episode_path)
    title_principals_df = title_principals_extract_transform(spark, title_principals_path)
    title_ratings_df = title_ratings_extract_transform(spark, title_ratings_path)

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
        title_basics_df, 
        title_ratings_df, 
        title_akas_df
    )
    print("Compute language rating trends")
    print(f"Total rows in resulting dataframe: {language_rating_trends_df.count()}")
    language_rating_trends_df.show(truncate=False, n=20)
    
    query_dolynska_result_df = query_dolynska(
        spark, 
        25, 
        title_crew_df, 
        title_basics_df, 
        title_ratings_df
    )
    print("Compute") # Todo: denys change naming
    print(f"Total rows in resulting dataframe: {query_dolynska_result_df.count()}")
    query_dolynska_result_df.show(truncate=False, n=20)

    top_genres_average_rating_over_decades_df = top_genres_average_rating_over_decades(
        title_basics_df,
        title_ratings_df, 
        top_n=5
    )
    print("Compute top genres average rating over decades")
    print(f"Total rows in resulting dataframe: {top_genres_average_rating_over_decades_df.count()}")
    top_genres_average_rating_over_decades_df.show(truncate=False, n=20)
    
    top_5_comedy_movies_df = top_comedy_movies_after_2010(
        title_basics, 
        title_ratings,
        title_akas
    )
    print("Compute top 5 comedy movies after 2010")
    print(f"Total rows in resulting dataframe: {top_5_comedy_movies_df.count()}")
    top_5_comedy_movies_df.show(truncate=False, n=20)
