from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count

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
    
    :param title_ratings_df: DataFrame з даними title.ratings
    :param title_principals_df: DataFrame з даними title.principals
    :param name_basics_df: DataFrame з даними name.basics
    :param top_n: Кількість топ фільмів за кількістю відгуків, що розглядаються
    :return: DataFrame з унікальним ідентифікатором актора (nconst), іменем (primaryName)
             та кількістю появ (film_count)
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
