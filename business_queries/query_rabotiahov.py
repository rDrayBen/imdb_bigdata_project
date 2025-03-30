from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, split, avg, count, floor, substring


def top_genres_average_rating_over_decades(
    title_basics_df: DataFrame, title_ratings_df: DataFrame, top_n: int
) -> DataFrame:
    """
    Computes the top N genres with the highest average rating and how their average rating
    changed over each decade.

    :param title_basics_df: DataFrame containing title.basics data
    :param title_ratings_df: DataFrame containing title.ratings data
    :param top_n: Number of top genres to consider
    :return: DataFrame with genres, decades, and average ratings
    """
    # Join the two dataframes on tconst
    joined_df = title_basics_df.join(title_ratings_df, on="tconst", how="inner")

    # Explode genres into individual rows
    exploded_df = joined_df.withColumn("genre", explode(split(col("genres"), ",")))

    # Filter out rows with null or empty genres
    filtered_df = exploded_df.filter(col("genre").isNotNull() & (col("genre") != "\\N"))

    # Extract the year from the startYear column (assumes format 'YYYY-MM-DD')
    filtered_df = filtered_df.withColumn("startYear", substring(col("startYear"), 1, 4).cast("int"))

    # Filter out rows with null or invalid startYear
    filtered_df = filtered_df.filter(col("startYear").isNotNull())

    # Filter to include only titles with startYear later than 1990
    filtered_df = filtered_df.filter(col("startYear") >= 1990)

    # Add a decade column by flooring the startYear to the nearest decade
    filtered_df = filtered_df.withColumn(
        "decade", (floor(col("startYear") / 10) * 10).cast("int")
    )

    # Compute the average rating for each genre
    genre_avg_rating_df = (
        filtered_df.groupBy("genre")
        .agg(avg("averageRating").alias("avg_rating"), count("tconst").alias("count"))
        .orderBy(col("avg_rating").desc())
    )

    # Get the top N genres
    top_genres = genre_avg_rating_df.limit(top_n).select("genre").rdd.flatMap(lambda x: x).collect()

    # Filter the original dataframe to include only the top N genres
    top_genres_df = filtered_df.filter(col("genre").isin(top_genres))

    # Compute the average rating for each genre and decade
    result_df = (
        top_genres_df.groupBy("genre", "decade")
        .agg(avg("averageRating").alias("avg_rating"))
        .orderBy("genre", "decade")
    )

    return result_df