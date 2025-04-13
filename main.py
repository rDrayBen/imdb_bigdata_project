from pyspark.sql import SparkSession
from business_queries.query_slobodian import high_rated_films_associated_actors, \
    most_frequent_interactions, most_localized_series, most_consistent_high_rated_series, \
    genres_with_lowest_completion_rate, detect_rating_drops


def main():
    spark = SparkSession.builder.appName("IMDB Analysis").config("spark.driver.memory", "4g").getOrCreate()
    print("Driver Memory:", spark.sparkContext._conf.get("spark.driver.memory"))

    title_episode_path = "./app/data/title.episode_cleaned.csv"
    title_episode_df = spark.read.csv(title_episode_path, sep="\t", header=True, inferSchema=True)

    title_basics_path = "./app/data/title_basics_cleaned.csv"
    title_basics_df = spark.read.csv(title_basics_path, sep="\t", header=True, inferSchema=True)

    title_principals_path = "./app/data/title.principals.cleaned.csv"
    title_principals_df = spark.read.csv(title_principals_path, sep="\t", header=True, inferSchema=True)

    name_basics_path = "./app/data/name.basics_cleaned.tsv"
    name_basics_df = spark.read.csv(name_basics_path, sep="\t", header=True, inferSchema=True)

    title_crew_path = "./app/data/title.crew.cleaned.tsv"
    title_crew_df = spark.read.csv(title_crew_path, sep="\t", header=True, inferSchema=True)

    title_akas_path = "./app/data/title.akas.cleaned.tsv"
    title_akas_df = spark.read.csv(title_akas_path, sep="\t", header=True, inferSchema=True)

    title_ratings_path = "./app/data/title.ratings.cleaned.tsv"
    title_ratings_df = spark.read.csv(title_ratings_path, sep="\t", header=True, inferSchema=True)

    title_episode_df = title_episode_df.select([col.strip() for col in title_episode_df.columns])
    title_basics_df = title_basics_df.select([col.strip() for col in title_basics_df.columns])
    title_principals_df = title_principals_df.select([col.strip() for col in title_principals_df.columns])
    name_basics_df = name_basics_df.select([col.strip() for col in name_basics_df.columns])
    title_crew_df = title_crew_df.select([col.strip() for col in title_crew_df.columns])
    title_akas_df = title_akas_df.select([col.strip() for col in title_akas_df.columns])
    title_ratings_df = title_ratings_df.select([col.strip() for col in title_ratings_df.columns])
    # Slobodian Main Query ===============================================================

    print("Title Akas Columns:", title_akas_df.columns)
    print("Title Akas total rows:\t", {title_akas_df.count()}, end='\n')

    print("Title Ratings Columns:", title_ratings_df.columns)
    print("Title Ratings total rows:\t", {title_ratings_df.count()}, end='\n')

    print("Title Principals Columns:", title_principals_df.columns)
    print("Title Principals total rows:\t", {title_principals_df.count()}, end='\n')

    print("Name Basics Columns:", name_basics_df.columns)
    print("Name Basics total rows:\t", {name_basics_df.count()}, end='\n')

    print("Title Basics Columns:", title_basics_df.columns)
    print("Title Basics total rows:\t", {title_basics_df.count()}, end='\n')

    print("Title Episode Columns:", title_episode_df.columns)
    print("Title Episode total rows:\t", {title_episode_df.count()}, end='\n')

    print("Title Crew Columns:", title_crew_df.columns)
    print("Title Crew total rows:\t", {title_crew_df.count()}, end='\n')

    result_df = high_rated_films_associated_actors(title_akas_df, title_ratings_df,
                                                   title_principals_df, name_basics_df, top_n=100)

    result_df.show(truncate=False, n=1000)

    # Slobodian Main Query ===============================================================
    # Slobodian Queries ==================================================================

    localized_series = most_localized_series(
        title_basics_df=title_basics_df,
        title_episode_df=title_episode_df,
        title_akas_df=title_akas_df,
        top_n=100
    )

    localized_series.show(truncate=False, n=1000)

    frequent_interactions = most_frequent_interactions(
        title_episode_df,
        title_basics_df,
        title_principals_df,
        title_crew_df,
        name_basics_df
    )

    frequent_interactions.show(truncate=False, n=1000)

    consistent_high_rated_series = most_consistent_high_rated_series(
        title_episode_df=title_episode_df,
        title_basics_df=title_basics_df,
        title_ratings_df=title_ratings_df,
        top_n=100
    )

    consistent_high_rated_series.show(truncate=False, n=1000)

    not_completed_genres = genres_with_lowest_completion_rate(
        title_episode_df=title_episode_df,
        title_basics_df=title_basics_df,
        top_n=100
    )

    not_completed_genres.show(truncate=False, n=1000)

    rating_drops = detect_rating_drops(
        title_episode_df,
        title_ratings_df,
        title_basics_df
    )

    rating_drops.show(truncate=False, n=1000)


if __name__ == '__main__':
    main()
