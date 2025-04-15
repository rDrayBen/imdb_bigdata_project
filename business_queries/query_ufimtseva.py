from pyspark.sql.functions import col, year, count, avg, min, max, stddev
from pyspark.sql import DataFrame
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, ntile

def compute_language_rating_trends(title_basics: DataFrame, title_ratings: DataFrame, title_akas: DataFrame):
    """
        Computes the trend of average movie ratings for each language based on original titles.
        It compares the average rating of movies released in the last 5 years with the all-time average rating for that language.

        Args:
            title_basics (DataFrame): A DataFrame containing movie metadata including columns:
                - tconst (string): Unique identifier for the title.
                - startYear (int): Year the movie was released.
                - genres (string): Genre(s) of the movie.

            title_ratings (DataFrame): A DataFrame containing movie ratings including columns:
                - tconst (string): Unique identifier for the title.
                - averageRating (float): Average user rating for the movie.

            title_akas (DataFrame): A DataFrame containing alternative titles including columns:
                - titleId (string): Unique identifier for the title (matches tconst in title_basics).
                - isOriginalTitle (int): Whether this is the original title (1 if true).
                - language (string): The language of the title.

        Returns:
            DataFrame: A DataFrame with the following columns:
                - language (string): The language of the original titles.
                - Avg_Rating_Last_5_Years (float): The average rating of movies released in the last 5 years for each language.
                - Avg_Rating_All_Time (float): The all-time average rating for each language.
                - Rating_Difference (float): The difference between the last 5 years' average rating and the all-time average rating.
        """
    original_titles = (title_akas.filter((col("language") != "Unknown"))
                       .select("titleId", "language"))

    title_basics = title_basics.filter((col("startYear").isNotNull()))

    movies_with_language = (title_basics.join(original_titles, title_basics.tconst == original_titles.titleId, "inner")
                            .select("tconst", "language", "startYear"))

    movies_with_ratings = (movies_with_language.join(title_ratings, "tconst", "inner")
                           .select("language", "startYear", "averageRating"))

    movies_with_ratings = movies_with_ratings.withColumn("startYear", year(col("startYear")))

    current_year = datetime.now().year

    last_5_years_ratings = movies_with_ratings.filter(
        col("startYear") >= (current_year - 5)) \
        .groupBy("language") \
        .agg(avg("averageRating").alias("Avg_Rating_Last_5_Years"))

    all_time_ratings = movies_with_ratings.groupBy("language").agg(avg("averageRating").alias("Avg_Rating_All_Time"))

    rating_trends = last_5_years_ratings.join(all_time_ratings, "language", "inner") \
        .withColumn("Rating_Difference", col("Avg_Rating_Last_5_Years") - col("Avg_Rating_All_Time"))

    return rating_trends


def get_titles_by_region(title_akas, title_basics, title_ratings):
    """
    Computes the average rating and number of movie titles released in the last 5 years per region.

    Args:
        title_akas (DataFrame): A DataFrame containing alternative titles including columns:
            - titleId (string): Unique identifier for the title (matches tconst in title_basics).
            - region (string): Region where the title was released.

        title_basics (DataFrame): A DataFrame containing movie metadata including columns:
            - tconst (string): Unique identifier for the title.
            - startYear (int): Year the movie was released.

        title_ratings (DataFrame): A DataFrame containing movie ratings including columns:
            - tconst (string): Unique identifier for the title.
            - averageRating (float): Average user rating for the movie.

    Returns:
        DataFrame: A DataFrame with the following columns:
            - region (string): The region of release.
            - num_original_titles (int): Number of original movie titles released in the region.
            - avg_rating (float): Average rating of those titles.
    """

    current_year = datetime.now().year

    titles = title_akas.filter(
        (col("isOriginalTitle") == 1) &
        (col("region") != "Unknown")
    ).select("titleId", "region")

    title_basics = title_basics.filter(col("startYear").isNotNull())
    title_basics = title_basics.withColumn("startYear", year(col("startYear")))


    recent_titles = title_basics.filter(
        col("startYear") >= (current_year - 5)
    ).select("tconst", "startYear")

    recent_original_titles = recent_titles.join(
        titles,
        recent_titles.tconst == titles.titleId,
        how="inner"
    ).select("tconst", "region")

    titles_with_ratings = recent_original_titles.join(
        title_ratings,
        "tconst",
        how="left"
    ).select("region", "averageRating")

    stats_by_region = titles_with_ratings.groupBy("region").agg(
        count("*").alias("num_original_titles"),
        avg("averageRating").alias("avg_rating")
    ).orderBy(col("num_original_titles").desc())

    return stats_by_region


def get_top_rated_alternative_titles(title_akas, title_ratings, title_basics):
    """
        Retrieves the top-rated alternative titles by region from the last 5 years.

        Args:
            title_akas (DataFrame): A DataFrame containing alternative titles including columns:
                - titleId (string): Unique identifier for the title.
                - region (string): Region where the title was released.

            title_ratings (DataFrame): A DataFrame containing movie ratings including columns:
                - tconst (string): Unique identifier for the title.
                - averageRating (float): Average user rating for the movie.

            title_basics (DataFrame): A DataFrame containing movie metadata including columns:
                - tconst (string): Unique identifier for the title.
                - startYear (int): Year the movie was released.

        Returns:
            DataFrame: A DataFrame with the following columns:
                - title (string): Alternative title of the movie.
                - region (string): Region of release.
                - averageRating (float): Rating of the movie.
                - rank_by_rating (int): Ranking within region by average rating.
        """

    current_year = datetime.now().year

    akas_recent = title_akas.filter(
        (col("region") != "Unknown")
    )

    title_basics = title_basics.filter((col("startYear").isNotNull()))
    title_basics = title_basics.withColumn("startYear", year(col("startYear")))

    akas_with_year = akas_recent.join(
        title_basics.select("tconst", "startYear"),
        akas_recent.titleId == title_basics.tconst,
        how="inner"
    ).filter(
        col("startYear") >= (current_year - 5)
    )

    akas_with_ratings = akas_with_year.join(
        title_ratings,
        akas_with_year.titleId == title_ratings.tconst,
        how="inner"
    ).select(
        col("title").alias("title"),
        col("region"),
        col("averageRating")
    )

    region_window = Window.partitionBy("region").orderBy(
        col("averageRating").desc())

    ranked = akas_with_ratings.withColumn(
        "rank_by_rating", rank().over(region_window)
    )

    return ranked.orderBy("region", "rank_by_rating")


def actors_in_high_rated_movies(title_akas, title_ratings, title_principals, name_basics):
    """
        Identifies actors most frequently appearing in high-rated movies across different regions.

        Args:
            title_akas (DataFrame): A DataFrame containing alternative titles including columns:
                - titleId (string): Unique identifier for the title.
                - region (string): Region where the title was released.

            title_ratings (DataFrame): A DataFrame containing movie ratings including columns:
                - tconst (string): Unique identifier for the title.
                - averageRating (float): Average user rating for the movie.

            title_principals (DataFrame): A DataFrame containing principal cast/crew information including columns:
                - tconst (string): Unique identifier for the title.
                - nconst (string): Unique identifier for a person.
                - category (string): Role category (e.g., actor, actress, director).

            name_basics (DataFrame): A DataFrame containing names including columns:
                - nconst (string): Unique identifier for a person.
                - primaryName (string): Person's name.

        Returns:
            DataFrame: A DataFrame with the following columns:
                - actor_name (string): Name of the actor/actress.
                - region (string): Region where their movie appeared.
                - appearances (int): Count of appearances in high-rated films.
        """
    high_rated = title_ratings.filter(col("averageRating") >= 7.5)

    akas_with_rating = (title_akas.filter(col("region") != "Unknown")
                        .join(high_rated, title_akas.titleId == high_rated.tconst, "inner")
                        .select("titleId", "region", "averageRating"))

    principals = (akas_with_rating.join(title_principals,
                                        akas_with_rating.titleId == title_principals.tconst,
                                        "inner")
                  .filter((col("category") == "actor") | (col("category") == "actress"))
                  .select("nconst", "region"))

    actors_with_names = (principals.join(name_basics, "nconst", "inner")
                         .select(col("primaryName")
                         .alias("actor_name"), "region"))

    appearances_by_region = (actors_with_names.groupBy("actor_name", "region")
                             .agg(count("*").alias("appearances"))
                             .orderBy(col("appearances").desc()))

    return appearances_by_region


def movies_quartiles_by_region(title_akas, title_basics, title_ratings):
    """
        Assigns movies to quartiles within each region based on their average rating.

        Args:
            title_akas (DataFrame): A DataFrame containing alternative titles including columns:
                - titleId (string): Unique identifier for the title.
                - region (string): Region where the title was released.

            title_basics (DataFrame): A DataFrame containing movie metadata including columns:
                - tconst (string): Unique identifier for the title.
                - primaryTitle (string): Title of the movie.

            title_ratings (DataFrame): A DataFrame containing movie ratings including columns:
                - tconst (string): Unique identifier for the title.
                - averageRating (float): Average user rating for the movie.

        Returns:
            DataFrame: A DataFrame with the following columns:
                - primaryTitle (string): Title of the movie.
                - region (string): Region where the movie is released.
                - averageRating (float): Average rating of the movie.
                - rating_quartile (int): Quartile (1-4) representing rating rank in the region.
        """
    akas = (title_akas.filter(col("region") != "Unknown")
            .select(col("titleId").alias("tconst"), "region"))

    joined = (
        akas.join(title_basics, "tconst", "inner").join(title_ratings, "tconst", "inner")
        .select("primaryTitle", "region", "averageRating"))

    region_window = Window.partitionBy("region").orderBy(
        col("averageRating").desc())

    result = joined.withColumn("rating_quartile", ntile(4).over(region_window))

    return result


def translations_by_region(title_akas, title_basics):
    """
        Calculates how often movies are translated into different regions.

        Args:
            title_akas (DataFrame): A DataFrame containing alternative titles including columns:
                - titleId (string): Unique identifier for the title.
                - region (string): Region where the title was released.

            title_basics (DataFrame): A DataFrame containing movie metadata including columns:
                - tconst (string): Unique identifier for the title.
                - originalTitle (string): Original title of the movie.

        Returns:
            DataFrame: A DataFrame with the following columns:
                - originalTitle (string): Original title of the movie.
                - region (string): Region where the movie was translated.
                - num_translations (int): Number of alternative translations per region.
        """
    akas_with_region = title_akas.filter(
        (col("region") != "Unknown") & (col("region").isNotNull())
    ).select("titleId", "region")

    akas_with_titles = akas_with_region.join(
        title_basics.select("tconst", "originalTitle"),
        akas_with_region.titleId == title_basics.tconst,
        how="inner"
    ).select("originalTitle", "region")

    translations_count = akas_with_titles.groupBy("originalTitle","region").agg(
        count("*").alias("num_translations")
    ).orderBy(col("num_translations").desc())

    return translations_count
