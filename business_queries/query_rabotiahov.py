from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, explode, split, avg, count, floor, substring
from pyspark.sql import functions as F


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

#############################################
# Question 1:
# “Визначте 10 найкращих режисерів із найвищим середнім рейтингом 
# (середнім рейтингом серед всіх вибраних фільмів цього режисера) фільмів для фільмів, 
# що мають щонайменше 1000 голосів.”
#############################################

def top_directors_avg_rating(title_basics_df: DataFrame, 
                             title_ratings_df: DataFrame, 
                             top_n: int = 10) -> DataFrame:
    joined_df = title_basics_df.join(title_ratings_df, on="tconst", how="inner")
    
    filtered_df = joined_df.filter((F.col("numVotes") >= 1000) & 
                                   (F.col("titleType") == "movie"))
    
    directors_df = filtered_df.withColumn("director", 
                                            F.explode(F.split(F.col("directors"), ",")))
    
    avg_rating_df = directors_df.groupBy("director") \
                                .agg(F.avg("averageRating").alias("avg_rating"))
    
    ranking_window = Window.orderBy(F.desc("avg_rating"))
    ranked_directors = avg_rating_df.withColumn("rank", F.row_number().over(ranking_window))
    
    top_directors = ranked_directors.filter(F.col("rank") <= top_n)
    
    return top_directors

#############################################
# Question 2:
# “Перелічіть найбільш роботящих акторів - тих, хто знімався у понад 50 фільмах 
# - з відповідними підрахунками фільмів.”
#############################################

def prolific_actors(title_principals_df: DataFrame, top_n: int = None) -> DataFrame:
    actors_df = title_principals_df.filter(F.col("category").isin("actor", "actress"))
    
    actor_counts = actors_df.groupBy("nconst") \
                            .agg(F.countDistinct("tconst").alias("film_count"))
    
    prolific_df = actor_counts.filter(F.col("film_count") > 50)
    
    ranking_window = Window.orderBy(F.desc("film_count"))
    ranked_actors = prolific_df.withColumn("rank", F.row_number().over(ranking_window))
    
    if top_n is not None:
        ranked_actors = ranked_actors.filter(F.col("rank") <= top_n)
    
    return ranked_actors

#############################################
# Question 3:
# “Для кожного фільму перелічіть імена акторів, замовлених за допомогою користувальницького рейтингу 
# (наприклад, порядок їх появи в акторському складі).”
#############################################

def movie_cast_ordered(title_principals_df: DataFrame, 
                       name_basics_df: DataFrame) -> DataFrame:
    cast_df = title_principals_df.filter(F.col("category").isin("actor", "actress"))
    
    cast_df = cast_df.join(name_basics_df, on="nconst", how="left")
    
    ordered_cast = cast_df.orderBy("tconst", "ordering")
    
    movie_cast = ordered_cast.groupBy("tconst") \
                             .agg(F.collect_list("primaryName").alias("cast_ordered"))
    return movie_cast

#############################################
# Question 4:
# “Ідентифікувати телесеріали (Titletype = 'TVSeries'), де кількість епізодів перевищує 
# загальну середню кількість епізодів на серіал із рейтингом більше N.”
#############################################

def tv_series_above_avg_eps(title_basics_df: DataFrame, 
                            title_ratings_df: DataFrame, 
                            rating_threshold: float) -> DataFrame:
    tv_df = title_basics_df.filter(F.col("titleType") == "TVSeries")
    
    tv_df = tv_df.join(title_ratings_df, on="tconst", how="inner")
    
    # Consider only series with an average rating above the provided threshold.
    filtered_tv = tv_df.filter(F.col("averageRating") > rating_threshold)
    
    avg_eps_val = filtered_tv.agg(F.avg(F.col("numEpisodes").cast("double")).alias("avg_eps")) \
                             .collect()[0]["avg_eps"]
    
    result_tv = filtered_tv.filter(F.col("numEpisodes").cast("double") > F.lit(avg_eps_val))
    
    return result_tv

#############################################
# Question 5:
# “Які серіали мають найбільшу кількість серій?”
#############################################

def series_with_most_episodes(title_basics_df: DataFrame) -> DataFrame:
    tv_series_df = title_basics_df.filter(F.col("titleType") == "TVSeries")
    
    most_episodes_series = tv_series_df.orderBy(F.desc(F.col("numEpisodes").cast("int")))
    
    return most_episodes_series

