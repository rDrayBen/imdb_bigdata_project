import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, substring, avg, count, floor, split, explode, min, max, lit, format_number, when, lag)
from pyspark.sql.window import Window


def round_to_5_years(year_col):
    return floor(col(year_col) / 5) * 5


def top_n_directors_rating_evolution(spark: SparkSession, top_n: int, title_crew, title_basics, title_ratings, name_basics):
    movies = (title_basics
              .filter(col("titleType") == "movie")
              .withColumn("startYear", substring("startYear", 1, 4).cast("int"))
              .select("tconst", "startYear"))

    movies_with_directors = title_crew.select("tconst", "directors")
    ratings = title_ratings.select("tconst", "averageRating")

    director_movies = (movies_with_directors
                       .withColumn("director_nconst", explode(split(col("directors"), ",")))
                       .join(name_basics, name_basics.nconst == col("director_nconst"))
                       .select("tconst", "director_nconst", "primaryName"))

    director_movies = director_movies.join(movies, "tconst")
    director_movies = director_movies.join(ratings, "tconst")

    top_n_directors = (director_movies
                       .groupBy("primaryName")
                       .agg(count("tconst").alias("film_count"))
                       .orderBy(col("film_count").desc())
                       .limit(top_n))

    director_movies = director_movies.join(top_n_directors,
                                           director_movies.primaryName == top_n_directors.primaryName).drop(
    top_n_directors.primaryName)

    first_last_years = (director_movies
                        .groupBy("primaryName")
                        .agg(min("startYear").alias("firstYear"), max("startYear").alias("lastYear")))

    director_movies = director_movies.join(first_last_years, "primaryName")

    # first_film_ratings = (director_movies.filter(col("startYear") == col("firstYear"))
    #                       .select("director", "averageRating")
    #                       .withColumnRenamed("averageRating", "firstFilmRating"))
    #
    # last_film_ratings = (director_movies.filter(col("startYear") == col("lastYear"))
    #                      .select("director", "averageRating")
    #                      .withColumnRenamed("averageRating", "lastFilmRating"))

    # director_movies = (director_movies
    #                    .join(first_film_ratings, "director", "left")
    #                    .join(last_film_ratings, "director", "left"))

    director_movies = director_movies.withColumn("careerPeriod", round_to_5_years("startYear"))
    periodic_avg_ratings = (director_movies
                            .groupBy("primaryName", "careerPeriod")
                            .agg(avg("averageRating").alias("periodicAvgRating")))

    min_window = director_movies.agg(min("careerPeriod")).collect()[0][0]
    max_window = director_movies.agg(max("careerPeriod")).collect()[0][0]

    five_years = list(range(min_window, max_window + 5, 5))
    five_years_df = spark.createDataFrame([(d,) for d in five_years], ["careerPeriod"])

    full_director_five_years = (top_n_directors
                                .crossJoin(five_years_df)
                                .join(periodic_avg_ratings, ["primaryName", "careerPeriod"], "left"))

    director_movies_pivoted = (full_director_five_years
                               .groupBy("primaryName", "film_count")
                               .pivot("careerPeriod", five_years)
                               .agg(avg("periodicAvgRating"))
                               .na.fill("-"))

    for year in range(min_window, max_window + 5, 5):
        col_name = f"{year}"
        formatted_col = f"{year}_{(year - (year // 100) * 100) + 4}"
        director_movies_pivoted = (
            director_movies_pivoted
            .withColumnRenamed(col_name, formatted_col)
            .withColumn(formatted_col, format_number(col(formatted_col), 2))
            .withColumn(formatted_col, when(col(formatted_col).isNotNull(), col(formatted_col)).otherwise(lit("-"))))

    director_movies_pivoted = director_movies_pivoted.na.fill("-")

    year_columns = director_movies_pivoted.columns[2:]
    long_df = director_movies_pivoted.select(
        'primaryName',
        F.explode(F.array(
            *[F.struct(F.lit(year).alias("year"), F.col(year).alias("rating")) for year in year_columns])).alias(
            "year_rating")
    ).select(
        'year_rating.year',
        'primaryName',
        'year_rating.rating'
    )

    pivoted_df = long_df.groupBy('year').pivot('primaryName').agg(F.first('rating'))

    pivoted_df_cleaned = pivoted_df
    for col_name in pivoted_df.columns:
        # cleaned_name = col_name.replace(" ", "_").replace(".", "")
        cleaned_name = col_name.replace(".", "")
        pivoted_df_cleaned = pivoted_df_cleaned.withColumnRenamed(col_name, cleaned_name)

    window_spec = Window.orderBy('year')
    result_df = pivoted_df_cleaned.select(
        'year',
        *[
            F.when(
                (F.col(director) == '-') & (F.lag(F.col(director)).over(window_spec) == '-'),
                '-'
            ).when(
                (F.col(director) != '-') & (F.lag(F.col(director)).over(window_spec) == '-'),
                F.col(director)
            ).when(
                (F.col(director) != '-') & (F.lag(F.col(director)).over(window_spec) != '-'),
                F.round(F.col(director) - F.lag(F.col(director)).over(window_spec), 2)
            ).otherwise(F.col(director)).alias(director)
            for director in pivoted_df_cleaned.columns[1:]
        ]
    )

    pivoted_df_cleaned = result_df.withColumnRenamed("Gilberto_Martínez_Solares", "Gilberto_Mart...")
    return pivoted_df_cleaned


if __name__ == '__main__':
    spark = (SparkSession.builder.appName("Directors productivity analysis")
             .config("spark.executor.memory", "4g")
             .config("spark.driver.memory", "4g")
             .getOrCreate())

    title_crew_path = os.path.abspath(f"../data/title.crew.cleaned.tsv")
    title_basics_path = os.path.abspath(f"../data/title.basics.cleaned.tsv")
    title_ratings_path = os.path.abspath(f"../data/title.ratings.cleaned.tsv")
    name_basics_path = os.path.abspath(f"../data/name.basics.cleaned.tsv")

    title_crew = spark.read.csv(title_crew_path, header=True, sep="\t")
    title_basics = spark.read.csv(title_basics_path, header=True, sep="\t")
    title_ratings = spark.read.csv(title_ratings_path, header=True, sep="\t")
    name_basics = spark.read.csv(name_basics_path, header=True, sep="\t")

    result_df = query_dolynska(spark, 10, title_crew, title_basics, title_ratings, name_basics)
    result_df.show(15, truncate=False)
