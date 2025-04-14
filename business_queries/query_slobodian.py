from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, desc

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

    # 3. Join with title.akas to get region info, filter valid regions
    high_rated_with_region_df = title_akas_df.join(
        high_rated_df, on="tconst", how="inner"
    ).filter(
        (col("region").isNotNull()) & (col("region") != "Unknown")
    )

    # 4. Filter only actors and actresses from title.principals
    principals_df = title_principals_df.filter(
        (col("category") == "actor") | (col("category") == "actress")
    )

    # 5. Join to get actor-region-film association
    films_actors_regions_df = high_rated_with_region_df.join(
        principals_df, on="tconst", how="inner"
    )

    # 6. Join with name.basics to get actor names
    actors_named_df = films_actors_regions_df.join(
        name_basics_df.select("nconst", "primaryName"), on="nconst", how="inner"
    )

    # 7. Group by actor name and region, count appearances
    appearances_df = actors_named_df.groupBy("primaryName", "region") \
        .agg(count("*").alias("appearances"))

    top_actors_df = appearances_df.orderBy(desc("appearances")).limit(top_n)

    return top_actors_df
