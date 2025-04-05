from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, split, avg, count, floor, substring


def top_genres_average_rating_over_decades(
    title_basics_df: DataFrame, title_ratings_df: DataFrame, top_n: int, decade_start: int = 1990
) -> DataFrame:
    """
    Computes the top N genres with the highest average rating and how their average rating
    changed over each decade.

    :param title_basics_df: DataFrame containing title.basics data
    :param title_ratings_df: DataFrame containing title.ratings data
    :param top_n: Number of top genres to consider
    :param decade_start: Starting year for filtering (default is 1990)
    :return: DataFrame with genres, decades, and average ratings
    """
    joined_df = title_basics_df.join(title_ratings_df, on="tconst", how="inner")
    exploded_df = joined_df.withColumn("genre", explode(split(col("genres"), ",")))
    filtered_df = exploded_df.filter(col("genre").isNotNull() & (col("genre") != "\\N"))
    filtered_df = filtered_df.withColumn("startYear", substring(col("startYear"), 1, 4).cast("int"))
    filtered_df = filtered_df.filter(col("startYear").isNotNull())
    filtered_df = filtered_df.filter(col("startYear") >= decade_start)
    filtered_df = filtered_df.withColumn(
        "decade", (floor(col("startYear") / 10) * 10).cast("int")
    )

    genre_avg_rating_df = (
        filtered_df.groupBy("genre")
        .agg(avg("averageRating").alias("avg_rating"), count("tconst").alias("count"))
        .orderBy(col("avg_rating").desc())
    )

    top_genres = genre_avg_rating_df.limit(top_n).select("genre").rdd.flatMap(lambda x: x).collect()
    top_genres_df = filtered_df.filter(col("genre").isin(top_genres))

    result_df = (
        top_genres_df.groupBy("genre", "decade")
        .agg(avg("averageRating").alias("avg_rating"))
        .orderBy("genre", "decade")
    )

    return result_df