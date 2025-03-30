from pyspark.sql import SparkSession
from business_queries.query_rabotiahov import top_genres_average_rating_over_decades


def main():
    # rabotiahov ===============================================================
    spark = SparkSession.builder.appName("IMDB Analysis").getOrCreate()

    # Read the title.basics DataFrame
    title_basics_path = "/app/data/title_basics.csv"
    title_basics_df = spark.read.csv(title_basics_path, sep="\t", header=True, inferSchema=True)

    # Read the title.ratings DataFrame
    title_ratings_path = "/app/data/title.ratings.cleaned.tsv"
    title_ratings_df = spark.read.csv(title_ratings_path, sep="\t", header=True, inferSchema=True)

    # Trim column names to remove leading/trailing spaces
    title_basics_df = title_basics_df.select([col.strip() for col in title_basics_df.columns])
    title_ratings_df = title_ratings_df.select([col.strip() for col in title_ratings_df.columns])

    print("Title Basics Columns:", title_basics_df.columns)
    print("Title Ratings Columns:", title_ratings_df.columns)
    print(f"Total Rows: {title_basics_df.count()}")

    # Call the function to compute the result
    top_n = 5  # You can change this value as needed
    result_df = top_genres_average_rating_over_decades(title_basics_df, title_ratings_df, top_n)

    # Show the result in the console
    result_df.show(truncate=False, n=1000)
    # rabotiahov ===============================================================


if __name__ == '__main__':
    main()
