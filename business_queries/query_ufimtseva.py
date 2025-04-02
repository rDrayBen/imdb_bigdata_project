from pyspark.sql.functions import col, avg, year
from pyspark.sql import DataFrame
from datetime import datetime

def compute_language_rating_trends(title_basics: DataFrame, title_ratings: DataFrame, title_akas: DataFrame):
    # Filter out rows where language is 'Unknown'
    original_titles = (title_akas.filter((col("language") != "Unknown"))
                       .select("titleId", "language"))

    # Filter out rows where startYear is NULL
    title_basics = title_basics.filter((col("startYear").isNotNull()))

    # Join with title_basics to get startYear
    movies_with_language = (title_basics.join(original_titles, title_basics.tconst == original_titles.titleId, "inner")
                            .select("tconst", "language", "startYear"))

    # Join with ratings to get averageRating
    movies_with_ratings = (movies_with_language.join(title_ratings, "tconst", "inner")
                           .select("language", "startYear", "averageRating"))

    # Extract a year from startYear column
    movies_with_ratings = movies_with_ratings.withColumn("startYear", year(col("startYear")))

    # Get the current year dynamically
    current_year = datetime.now().year

    # Compute last 5 years' average rating per language
    last_5_years_ratings = movies_with_ratings.filter(
        col("startYear") >= (current_year - 5)) \
        .groupBy("language") \
        .agg(avg("averageRating").alias("Avg_Rating_Last_5_Years"))

    # Compute all-time average rating per language
    all_time_ratings = movies_with_ratings.groupBy("language").agg(avg("averageRating").alias("Avg_Rating_All_Time"))

    # Join both results and compute rating difference
    rating_trends = last_5_years_ratings.join(all_time_ratings, "language", "inner") \
        .withColumn("Rating_Difference", col("Avg_Rating_Last_5_Years") - col("Avg_Rating_All_Time"))

    return rating_trends
