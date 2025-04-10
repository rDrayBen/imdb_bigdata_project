import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, substring, avg, count, floor, split, explode, min, max, lit, format_number, when, lag)
from pyspark.sql.window import Window


def round_to_5_years(year_col):
    return floor(col(year_col) / 5) * 5


def query_dolynska(spark: SparkSession, top_n: int, title_crew, title_basics, title_ratings):
    movies = (title_basics
              .filter(col("titleType") == "movie")
              .withColumn("startYear", substring("startYear", 1, 4).cast("int"))
              .select("tconst", "startYear"))

    movies_with_directors = title_crew.select("tconst", "directors")
    ratings = title_ratings.select("tconst", "averageRating")

    director_movies = (movies_with_directors
                       .withColumn("director", explode(split(col("directors"), ",")))
                       .join(movies, "tconst")
                       .join(ratings, "tconst"))

    top_n_directors = (director_movies
                       .groupBy("director")
                       .agg(count("tconst").alias("film_count"))
                       .orderBy(col("film_count").desc())
                       .limit(top_n))

    director_movies = director_movies.join(top_n_directors, "director")

    first_last_years = (director_movies
                        .groupBy("director")
                        .agg(min("startYear").alias("firstYear"), max("startYear").alias("lastYear")))

    director_movies = director_movies.join(first_last_years, "director")

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
                            .groupBy("director", "careerPeriod")
                            .agg(avg("averageRating").alias("periodicAvgRating")))

    min_window = director_movies.agg(min("careerPeriod")).collect()[0][0]
    max_window = director_movies.agg(max("careerPeriod")).collect()[0][0]

    five_years = list(range(min_window, max_window + 5, 5))
    five_years_df = spark.createDataFrame([(d,) for d in five_years], ["careerPeriod"])

    full_director_five_years = (top_n_directors
                                .crossJoin(five_years_df)
                                .join(periodic_avg_ratings, ["director", "careerPeriod"], "left"))
    director_movies_pivoted = (full_director_five_years
                               .groupBy("director", "film_count")
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
            .withColumn(formatted_col, when(col(formatted_col).isNotNull(), col(formatted_col)).otherwise(lit("-")))
        )

    director_movies_pivoted = director_movies_pivoted.na.fill("-")
    # director_movies_pivoted.show(n=top_n, truncate=False)

    year_columns = director_movies_pivoted.columns[2:]
    long_df = director_movies_pivoted.select(
        'director',
        F.explode(F.array(
            *[F.struct(F.lit(year).alias("year"), F.col(year).alias("rating")) for year in year_columns])).alias(
            "year_rating")
    ).select(
        'year_rating.year',
        'director',
        'year_rating.rating'
    )

    pivoted_df = long_df.groupBy('year').pivot('director').agg(F.first('rating'))
    # pivoted_df.show(len(year_columns), truncate=False)

    window_spec = Window.orderBy('year')
    result_df = pivoted_df.select(
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
            for director in pivoted_df.columns[1:]
        ]
    )

    return result_df


if __name__ == '__main__':
    spark = (SparkSession.builder.appName("Directors productivity analysis")
             .config("spark.executor.memory", "4g")
             .config("spark.driver.memory", "4g")
             .getOrCreate())

    title_crew_path = os.path.abspath(f"../data/title.crew.cleaned.tsv")
    title_basics_path = os.path.abspath(f"../data/title.basics.cleaned.tsv")
    title_ratings_path = os.path.abspath(f"../data/title.ratings.cleaned.tsv")

    title_crew = spark.read.csv(title_crew_path, header=True, sep="\t")
    title_basics = spark.read.csv(title_basics_path, header=True, sep="\t")
    title_ratings = spark.read.csv(title_ratings_path, header=True, sep="\t")

    result_df = query_dolynska(spark, 25, title_crew, title_basics, title_ratings)
    result_df.show(35, truncate=False)
