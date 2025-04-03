from pyspark.sql.functions import col, avg, year
from pyspark.sql import DataFrame
from datetime import datetime

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
