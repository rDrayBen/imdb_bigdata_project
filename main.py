from pyspark.sql import SparkSession

from business_queries.query_dolynska import query_dolynska
from data_preparation.title_crew_extract_transform import title_crew_extract_transform
from data_preparation.title_ratings_extract_transform import title_ratings_extract_transform


def main():
    spark = SparkSession.builder.appName("IMDb Crew Data Processing").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    title_ratings_df = title_ratings_extract_transform(spark, path="../data/title.ratings.tsv")
    title_crew_df = title_crew_extract_transform(spark, path="../data/title.crew.tsv")
    title_basics_df = title_basics_extract_transform(spark, path="../data/title.crew.tsv")
    # title_basics_path = os.path.abspath(f"../data/title.basics.cleaned.tsv")
    # title_basics_df = spark.read.csv(title_basics_path, header=True, sep="\t")

    query_dolynska_result_df = query_dolynska(spark, 25, title_crew_df, title_basics_df, title_ratings_df)
    query_dolynska_result_df.show(35, truncate=False)


if __name__ == '__main__':
    main()
    