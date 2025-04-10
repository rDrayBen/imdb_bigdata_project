from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame


def top_comedy_movies_after_2010(title_basics: DataFrame, title_ratings: DataFrame, title_akas: DataFrame):
    """
    Computes the top 5 comedy movies released after 2010 based on average rating,
    excluding russian adaptations.

    Args:
        title_basics (DataFrame): 
            - tconst (string): Unique identifier for the title.
            - primaryTitle (string): Title of the movie.
            - startYear (int): Year the movie was released.
            - genres (string): Genre(s) of the movie (comma-separated).
            - titleType (string): Type of title (e.g., "movie").
        
        title_ratings (DataFrame): 
            - tconst (string): Unique identifier for the title.
            - averageRating (float): Average user rating for the movie.
            - numVotes (int): Number of user votes.
        
        title_akas (DataFrame): 
            - titleId (string): Unique identifier for the title (matches tconst in title_basics).
            - language (string): The language of the title.

    Returns:
        DataFrame: A DataFrame with the following columns:
            - tconst (string): Unique identifier for the title.
            - primaryTitle (string): Title of the movie.
            - startYear (int): Year the movie was released.
            - genres (array): List of genres of the movie.
            - languages (array): List of languages for the movie.
            - numVotes (int): Number of votes for the movie.
            - averageRating (float): Average user rating for the movie.
    """
    

    filtered_movies = (
        title_basics
        .filter((F.col("startYear") > "2010") & (F.col("titleType") == "movie"))
        .withColumn("genres", F.split(F.col("genres"), ","))
        .filter(F.array_contains(F.col("genres"), "Comedy"))
    )
    
    russian_adaptations = title_akas.filter((F.col("language") == "ru")) 
    filtered_movies = filtered_movies.join(russian_adaptations, 
                                           filtered_movies.tconst == russian_adaptations.titleId, "leftanti")
    
    movies_with_ratings = (
        filtered_movies
        .join(title_ratings, "tconst")
        .select("tconst", "primaryTitle", "startYear", "genres", "numVotes", "averageRating")
        .filter(F.col("numVotes") > 500)
    )

    window_spec = Window.partitionBy("startYear").orderBy(F.col("averageRating").desc())

    top_movies = (
        movies_with_ratings
        .withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") <= 5)
        .select("tconst", "primaryTitle", "startYear", "genres", "numVotes", "averageRating")
    )

    top_movies_with_akas = top_movies.join(
        title_akas, top_movies.tconst == title_akas.titleId, "inner"
    )

    movies_with_languages = (
        top_movies_with_akas
        .filter(F.col("language") != "Unknown")
        .select("tconst", "primaryTitle", "startYear", "genres", "country", "language")
    )

    movies_grouped = movies_with_languages.groupBy("tconst").agg(
        F.collect_set("language").alias("languages")
    )

    final_movies = (
        top_movies
        .join(movies_grouped, "tconst", "inner")
        .select("tconst", "primaryTitle", "startYear", "genres", "languages", "numVotes", "averageRating")
    )

    final_movies_sorted = final_movies.orderBy(F.col("averageRating").desc())

    return final_movies_sorted
