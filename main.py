from pyspark.sql import SparkSession
from business_queries.query_slobodian import high_rated_films_associated_actors


def main():
    # slobodian ===============================================================
    spark = SparkSession.builder.appName("IMDB Analysis").config("spark.driver.memory", "4g").getOrCreate()
    print("Driver Memory:", spark.sparkContext._conf.get("spark.driver.memory"))
    # Read the title.akas DataFrame
    title_akas_path = "./app/data/title.akas.cleaned.tsv"
    title_akas_df = spark.read.csv(title_akas_path, sep="\t", header=True, inferSchema=True)

    # Read the title.ratings DataFrame
    title_ratings_path = "./app/data/title.ratings.cleaned.tsv"
    title_ratings_df = spark.read.csv(title_ratings_path, sep="\t", header=True, inferSchema=True)

    # Read the title.principals DataFrame
    title_principals_path = "./app/data/title.principals.cleaned.csv"
    title_principals_df = spark.read.csv(title_principals_path, sep="\t", header=True, inferSchema=True)

    # Read the name.basics DataFrame
    name_basics_path = "./app/data/name.basics_cleaned.tsv"
    name_basics_df = spark.read.csv(name_basics_path, sep="\t", header=True, inferSchema=True)

    # Trim column names to remove leading/trailing spaces
    title_akas_df = title_akas_df.select([col.strip() for col in title_akas_df.columns])
    title_ratings_df = title_ratings_df.select([col.strip() for col in title_ratings_df.columns])
    title_principals_df = title_principals_df.select([col.strip() for col in title_principals_df.columns])
    name_basics_df = name_basics_df.select([col.strip() for col in name_basics_df.columns])

    print("Title Akas Columns:", title_akas_df.columns)
    print("Title Akas total rows:\t", {title_akas_df.count()}, end='\n')

    print("Title Ratings Columns:", title_ratings_df.columns)
    print("Title Ratings total rows:\t", {title_ratings_df.count()}, end='\n')

    print("Title Principals Columns:", title_principals_df.columns)
    print("Title Principals total rows:\t", {title_principals_df.count()}, end='\n')

    print("Name Basics Columns:", name_basics_df.columns)
    print("Name Basics total rows:\t", {name_basics_df.count()}, end='\n')

    top_n = 100

    result_df = high_rated_films_associated_actors(title_akas_df, title_ratings_df,
                                                   title_principals_df, name_basics_df, top_n)

    result_df.show(truncate=False, n=1000)

    # slobodian ===============================================================


if __name__ == '__main__':
    main()
