from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame


def top_comedy_movies_after_2010(title_basics: DataFrame, title_ratings: DataFrame, title_akas: DataFrame):
    """
    Computes the top comedy movies released after 2010 based on average rating,
    excluding german adaptations.

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
    
    german_adaptations = title_akas.filter((F.col("language") == "de")) 
    filtered_movies = filtered_movies.join(german_adaptations, 
                                           filtered_movies.tconst == german_adaptations.titleId, "leftanti")
    
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
    
    final_movies_sorted = final_movies_sorted.withColumn("languages", F.concat_ws(",", F.col("languages")))
    final_movies_sorted = final_movies_sorted.withColumn("genres", F.concat_ws(",", F.col("genres")))

    return final_movies_sorted


def movies_after_2005(title_basics):
    return title_basics.filter(
        (F.col("titleType") == "movie") & (F.col("startYear") > 2005)
    )


def count_movies_by_genre(title_basics):
    return title_basics.filter(F.col("titleType") == "movie") \
        .withColumn("genre", F.explode(F.split(F.col("genres"), ","))) \
        .groupBy("genre") \
        .agg(F.count("*").alias("movie_count"))


def avg_duration_and_rating_by_genre(title_basics, title_ratings, n=100):
    movies = title_basics.filter(
        (F.col("titleType") == "movie") & F.col("runtimeMinutes").isNotNull()
    ).withColumn("genre", F.explode(F.split(F.col("genres"), ",")))

    joined = movies.join(title_ratings, on="tconst")

    grouped = joined.groupBy("genre").agg(
        F.count("*").alias("movie_count"),
        F.round(F.avg("runtimeMinutes"), 2).alias("avg_duration"),
        F.round(F.avg("averageRating"), 2).alias("avg_rating")
    ).filter(F.col("movie_count") >= n)

    return grouped


def top_5_longest_movies(title_basics):
    return title_basics.filter(
        (F.col("titleType") == "movie") & F.col("runtimeMinutes").isNotNull()
    ).orderBy(F.col("runtimeMinutes").desc()) \
        .select("primaryTitle", "runtimeMinutes") \
        .limit(5)


def most_productive_years(title_basics):
    return title_basics.filter(
        (F.col("titleType") == "movie") & F.col("startYear").isNotNull()
    ).groupBy("startYear") \
        .agg(F.count("*").alias("movie_count")) \
        .orderBy(F.col("movie_count").desc())


def top_directors_by_decade_with_region(
    title_basics,
    title_crew,
    title_ratings,
    title_akas,
    region_filter,
    genres_filter=None,
    min_votes=50,
    top_n=3
):
    movies = title_basics.filter(
        (F.col("titleType") == "movie") & F.col("startYear").isNotNull()
    )

    if genres_filter:
        movies = movies.withColumn("genre", F.explode(F.split(F.col("genres"), ","))) \
            .filter(F.col("genre").isin(genres_filter))

    movies = movies.withColumn(
        "decade", (F.col("startYear") / 10).cast("int") * 10
    )

    region_titles = title_akas.filter(
        F.col("region") == region_filter
    ).select(F.col("titleId").alias("tconst")).distinct()

    joined = movies.join(region_titles, on="tconst") \
        .join(title_crew, on="tconst", how="left") \
        .join(title_ratings.filter(F.col("numVotes") >= min_votes), on="tconst", how="inner")

    joined = joined.withColumn("director", F.explode(F.split(F.col("directors"), ","))) \
        .filter(F.col("director").isNotNull())

    grouped = joined.groupBy("decade", "director").agg(
        F.avg("averageRating").alias("avg_rating"),
        F.count("*").alias("movie_count")
    )

    window_spec = Window.partitionBy("decade").orderBy(F.desc("avg_rating"))

    return grouped.withColumn("rank", F.row_number().over(window_spec)) \
        .filter(F.col("rank") <= top_n)
