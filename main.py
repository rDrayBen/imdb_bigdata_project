from pyspark.sql import SparkSession

from data_preparation.name_basics_extract_transform import name_basics_extract_transform

from business_queries.query_ratushniak import count_actors_in_low_rated_popular_films

def main():
    spark = SparkSession.builder \
        .appName("IMDB Data Analysis") \
        .getOrCreate()
    
    # Load and transform data
    name_basics_data_path = "data/name.basics.tsv"
    name_basics_df = name_basics_extract_transform(spark, name_basics_data_path)

    # Perform business queries
    actors_in_low_rated_popular_films = count_actors_in_low_rated_popular_films(
        title_ratings_df, 
        title_principals_df, 
        name_basics_df, 
        average_rating=5.0, 
        top_n=5000
    )

    print("Count actors in low rated popular films")
    print(f"Total rows in resulting dataframe: {actors_in_low_rated_popular_films.count()}")
    actors_in_low_rated_popular_films.show(truncate=False, n=20)

if __name__ == '__main__':
    main()