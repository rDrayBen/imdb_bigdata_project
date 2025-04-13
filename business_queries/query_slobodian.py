from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, desc, split, avg, stddev, expr, lag
from pyspark.sql.window import Window

"""
3. Which actors are most frequently associated with high-rated films across multiple regions,
 and how do their appearances vary by region? (Join 'title.akas' with 'title.ratings' 
 to get region-specific films, and then join with 'title.principals' to find the actors involved) +
join(+), group by(+), filter(+)
"""


def high_rated_films_associated_actors(
        title_akas_df: DataFrame,
        title_ratings_df: DataFrame,
        title_principals_df: DataFrame,
        name_basics_df: DataFrame,
        top_n: int
) -> DataFrame:
    title_akas_df = title_akas_df.withColumnRenamed("titleId", "tconst")

    high_rated_df = title_ratings_df.filter(
        (col("averageRating") > 8.0) & (col("numVotes") > 1000000)
    )

    high_rated_with_region_df = title_akas_df.join(
        high_rated_df, on="tconst", how="inner").filter(
        (col("region").isNotNull()) & (col("region") != "Unknown")
    )

    principals_df = title_principals_df.filter(
        (col("category") == "actor") | (col("category") == "actress")
    )

    films_actors_regions_df = high_rated_with_region_df.join(
        principals_df, on="tconst", how="inner"
    )

    actors_named_df = films_actors_regions_df.join(
        name_basics_df.select("nconst", "primaryName"), on="nconst", how="inner"
    )

    appearances_df = actors_named_df.groupBy("primaryName", "region") \
        .agg(count("*").alias("appearances"))

    top_actors_df = appearances_df.orderBy(desc("appearances")).limit(top_n)

    return top_actors_df


def most_frequent_interactions(
        title_episode_df: DataFrame,
        title_basics_df: DataFrame,
        title_principals_df: DataFrame,
        title_crew_df: DataFrame,
        name_basics_df: DataFrame,
) -> DataFrame:
    from pyspark.sql.functions import col, explode, countDistinct

    episodes_df = title_episode_df.join(
        title_basics_df.filter(col("titleType") == "tvSeries")
            .select(col("tconst").alias("series_id")),
            title_episode_df["parentTconst"] == col("series_id"),
        how="inner"
    ).select("tconst", "parentTconst")

    episode_directors_df = episodes_df.join(
        title_crew_df.select("tconst", "directors"),
        on="tconst", how="inner"
    ).filter(col("directors").isNotNull())

    episode_directors_df = episode_directors_df.withColumn("director", explode(split(col("directors"), ",")))

    actors_df = title_principals_df.filter(
        (col("category") == "actor") | (col("category") == "actress")
    ).select("tconst", "nconst")

    director_actor_df = episode_directors_df.join(
        actors_df, on="tconst", how="inner"
    )

    repeated_collabs_df = director_actor_df.groupBy("director", "nconst") \
        .agg(countDistinct("parentTconst").alias("num_series"))

    frequent_pairs_df = repeated_collabs_df.filter(col("num_series") > 1)

    frequent_pairs_named_df = frequent_pairs_df \
        .join(name_basics_df.selectExpr("nconst as actor_id", "primaryName as actor_name"),
              on=col("nconst") == col("actor_id")) \
        .join(name_basics_df.selectExpr("nconst as director_id", "primaryName as director_name"),
              on=col("director") == col("director_id")) \
        .select("director_name", "actor_name", "num_series") \
        .orderBy(col("num_series").desc())

    return frequent_pairs_named_df


def most_localized_series(
        title_basics_df: DataFrame,
        title_episode_df: DataFrame,
        title_akas_df: DataFrame,
        top_n: int = 100
) -> DataFrame:
    from pyspark.sql.functions import col, countDistinct

    tv_series_df = title_basics_df.filter(col("titleType") == "tvSeries") \
        .select("tconst", "primaryTitle") \
        .withColumnRenamed("tconst", "series_id")

    episodes_df = title_episode_df.select("tconst", "parentTconst") \
        .withColumnRenamed("parentTconst", "series_id")

    episode_series_df = episodes_df.join(tv_series_df, on="series_id", how="inner")

    akas_df = title_akas_df.select("titleId", "region") \
        .withColumnRenamed("titleId", "tconst")

    localized_titles_df = episode_series_df.join(akas_df, on="tconst", how="inner") \
        .filter(col("region").isNotNull() & (col("region") != "Unknown"))

    series_localization_count_df = localized_titles_df.groupBy("series_id", "primaryTitle") \
        .agg(countDistinct("region").alias("num_localizations")) \
        .orderBy(col("num_localizations").desc()) \
        .limit(top_n)

    return series_localization_count_df


def most_consistent_high_rated_series(
        title_episode_df: DataFrame,
        title_ratings_df: DataFrame,
        title_basics_df: DataFrame,
        min_episodes: int = 3,
        min_votes: int = 100000,
        top_n: int = 10
) -> DataFrame:
    from pyspark.sql.functions import col, avg, stddev, count, sum as _sum

    episodes_with_ratings_df = title_episode_df.join(
        title_ratings_df, on="tconst", how="inner"
    ).select("parentTconst", "averageRating", "numVotes")

    series_stats_df = episodes_with_ratings_df.groupBy("parentTconst") \
        .agg(
        avg("averageRating").alias("avg_rating"),
        stddev("averageRating").alias("stddev_rating"),
        count("*").alias("episode_count"),
        _sum("numVotes").alias("total_votes")
    ).filter(
        (col("episode_count") >= min_episodes) &
        (col("total_votes") >= min_votes)
    )

    result_df = series_stats_df.join(
        title_basics_df.filter(col("titleType") == "tvSeries")
            .select(col("tconst").alias("parentTconst"), col("primaryTitle")),
        on="parentTconst",
        how="inner"
    )

    result_df = result_df.orderBy(col("avg_rating").desc(), col("stddev_rating").asc())

    return result_df.limit(top_n)


def genres_with_lowest_completion_rate(
        title_episode_df: DataFrame,
        title_basics_df: DataFrame,
        top_n: int = 10
) -> DataFrame:
    from pyspark.sql.functions import col, explode, split, count, sum as _sum, when

    series_df = title_episode_df.select("parentTconst").distinct() \
        .join(
        title_basics_df.filter(col("titleType") == "tvSeries")
            .select("tconst", "endYear", "genres"),
            title_episode_df["parentTconst"] == col("tconst"),
        how="inner"
    ).filter(col("genres").isNotNull())

    exploded_genres_df = series_df.withColumn("genre", explode(split(col("genres"), ",")))

    genre_status_df = exploded_genres_df.withColumn(
        "is_completed", when(col("endYear").isNotNull(), 1).otherwise(0)
    )

    genre_stats_df = genre_status_df.groupBy("genre") \
        .agg(
        count("*").alias("total_series"),
        _sum("is_completed").alias("completed_series")
    ).withColumn(
        "completion_rate", (col("completed_series") / col("total_series")) * 100
    ).withColumn(
        "incomplete_rate", 100 - col("completion_rate")
    ).orderBy(col("incomplete_rate").desc())

    return genre_stats_df.limit(top_n)


def detect_rating_drops(
        title_episode_df: DataFrame,
        title_ratings_df: DataFrame,
        title_basics_df: DataFrame
) -> DataFrame:
    episodes_with_ratings_df = title_episode_df \
        .join(title_ratings_df, on="tconst", how="inner") \
        .filter(col("seasonNumber").isNotNull() & col("episodeNumber").isNotNull() & (col("numVotes") >= 1000))

    episodes_with_titles_df = episodes_with_ratings_df \
        .join(title_basics_df.select("tconst", "primaryTitle"),
              episodes_with_ratings_df["parentTconst"] == title_basics_df["tconst"]) \
        .select(
        episodes_with_ratings_df["tconst"],
        col("parentTconst"),
        col("seasonNumber"),
        col("episodeNumber"),
        col("averageRating"),
        col("numVotes"),
        col("primaryTitle").alias("seriesTitle")
    )

    rating_window = Window.partitionBy("parentTconst").orderBy("seasonNumber", "episodeNumber")

    episodes_with_diff = episodes_with_titles_df \
        .withColumn("previousRating", lag("averageRating").over(rating_window)) \
        .withColumn("ratingDrop", col("previousRating") - col("averageRating")) \
        .filter(col("ratingDrop") > 0) \
        .orderBy(col("ratingDrop").desc())

    return episodes_with_diff
