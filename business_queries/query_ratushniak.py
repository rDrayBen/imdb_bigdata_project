from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, array_contains, explode, row_number, desc, countDistinct, split
from pyspark.sql.window import Window

def count_actors_in_low_rated_popular_films(
    title_ratings_df: DataFrame,
    title_principals_df: DataFrame,
    name_basics_df: DataFrame,
    average_rating: float,
    top_n: int
) -> DataFrame:
    """
    Повертає DataFrame з акторами (actor/actress), які найчастіше з'являються 
    у фільмах з середньою оцінкою нижче average_rating, але які входять до top_N фільмів 
    за кількістю відгуків.
    """

    result_df = (
        title_ratings_df.filter(col("averageRating") < average_rating)
        .join(
            title_ratings_df.orderBy(col("numVotes").desc()).limit(top_n),
            on="tconst",
            how="inner"
        )
        .join(
            title_principals_df.filter(col("category").isin(["actor", "actress"])),
            on="tconst",
            how="inner"
        )
        # Актор може з'являтися декілька раз у фільмі, видяляємо дублікати
        .dropDuplicates(["nconst", "tconst"])
        .groupBy("nconst")
        .agg(count("tconst").alias("film_count"))
        .join(
            name_basics_df.select("nconst", "primaryName"),
            on="nconst",
            how="left"
        )
        .orderBy(col("film_count").desc())
        .select("primaryName", "film_count")
    )
    
    return result_df

def topn_writers_by_drama_tv_episode_count(
        title_episode_df: DataFrame,
        title_basics_df: DataFrame,
        title_crew_df: DataFrame,
        name_basics_df: DataFrame,
        top_n: int,
    ) -> DataFrame:
    """
    Повертає DataFrame з top_n письменниками за кількістю написаних серій у телесеріалах з жанром "Drama".
    """
    tv_series = title_basics_df.filter((col("titleType") == "tvSeries") &
                                       (array_contains(split(col("genres"), ","), "Drama")))
    return (
        title_episode_df.join(tv_series, title_episode_df.parentTconst == tv_series.tconst, "inner")
        .join(title_crew_df, on="tconst", how="inner")
        .withColumn("writer", explode(col("writers")))
        .groupBy("writer")
        .agg(count("tconst").alias("episode_count"))
        .withColumn("rank", row_number().over(Window.orderBy(desc("episode_count"))))
        .filter(col("rank") <= top_n)
        .join(
            name_basics_df.select(col("nconst").alias("writer"), "primaryName"),
            on="writer", how="left"
        )
        .orderBy(desc("episode_count"))
        .select("primaryName", "episode_count")
    )

def actors_in_multilanguage_films(
        title_akas_df: DataFrame,
        title_principals_df: DataFrame,
        name_basics_df: DataFrame,
        languages_cnt: int
    ) -> DataFrame:
    """
    Повертає DataFrame з акторами, які мають найбільшу кількість фільмів, 
    перекладених більш ніж на languages_cnt мов.
    """
    return (
        title_akas_df.groupBy("titleId")
        .agg(countDistinct("language").alias("language_count"))
        .filter(col("language_count") > languages_cnt)
        .join(
            title_principals_df.filter(col("category").isin(["actor", "actress"])),
            title_akas_df.titleId == title_principals_df.tconst, "inner"
        )
        .dropDuplicates(["nconst", "tconst"])
        .groupBy("nconst")
        .agg(count("tconst").alias("film_count"))
        .join(
            name_basics_df.select("nconst", "primaryName"),
            on="nconst", how="left"
        )
        .orderBy(desc("film_count"))
        .select("primaryName", "film_count")
    )

def screenwriters_in_low_rated_high_votes_films(
        title_ratings_df: DataFrame,
        title_basics_df: DataFrame,
        title_crew_df: DataFrame,
        name_basics_df: DataFrame,
        average_rating: float,
        voices_cnt: int,
    ) -> DataFrame:
    """
    Повертає DataFrame з сценаристами, які писали сценарії для фільмів 
    з оцінкою нижче average_rating та понад voices_cnt голосів.
    """
    return (
        title_ratings_df.filter((col("averageRating") < average_rating) & (col("numVotes") > voices_cnt))
        .join(
            title_basics_df.filter(col("titleType") == "movie"),
            on="tconst", how="inner"
        )
        .join(title_crew_df, on="tconst", how="inner")
        .withColumn("writer", explode(col("writers")))
        .groupBy("writer")
        .agg(count("tconst").alias("film_count"))
        .join(
            name_basics_df.select(col("nconst").alias("writer"), "primaryName"),
            on="writer", how="left"
        )
        .orderBy(desc("film_count"))
        .select("primaryName", "film_count")
    )

def longest_consecutive_release_streak_screenwriters(
        title_basics_df: DataFrame,
        title_crew_df: DataFrame,
        name_basics_df: DataFrame
    ) -> DataFrame:
    """
    Повертає DataFrame зі сценаристами, які мали найдовшу послідовність випуску фільмів у послідовних роках.
    """
    return (
        title_crew_df.join(title_basics_df, on="tconst", how="inner")
        .filter((col("startYear").isNotNull()) & (col("startYear") != "\\N"))
        .withColumn("year", col("startYear").cast("int"))
        .withColumn("writer", explode(col("writers")))
        .select("writer", "year").dropDuplicates()
        .withColumn("rn", row_number().over(Window.partitionBy("writer").orderBy("year")))
        .withColumn("grp", col("year") - col("rn"))
        .groupBy("writer", "grp")
        .agg(count("year").alias("streak_length"))
        .groupBy("writer")
        .agg({"streak_length": "max"})
        .withColumnRenamed("max(streak_length)", "consecutive_years")
        .join(
            name_basics_df.select(col("nconst").alias("writer"), "primaryName"),
            on="writer", how="left"
        )
        .orderBy(desc("consecutive_years"))
        .select("primaryName", "consecutive_years")
    )

def common_professions_in_short_films(
        title_basics_df: DataFrame,
        title_principals_df: DataFrame,
        name_basics_df: DataFrame
    ) -> DataFrame:
    """
    Повертає DataFrame з професіями (primaryProfession), які найчастіше зустрічаються 
    у людей у короткометражних фільмах (titleType='short').
    """
    return (
        title_basics_df.filter(col("titleType") == "short")
        .join(title_principals_df, on="tconst", how="inner")
        .join(name_basics_df, on="nconst", how="left")
        .withColumn("profession", explode(col("primaryProfession")))
        .groupBy("profession")
        .agg(count("*").alias("count"))
        .orderBy(desc("count"))
        .select("profession", "count")
    )
