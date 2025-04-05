from business_queries import query_rabotiahov
from data_preparation import title_principals_extract_transform
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("IMDB Data Analysis") \
        .getOrCreate()
    
    # Load and transform title principals data
    title_principals_data_path = "data/title.principals.tsv"
    title_principals_df = title_principals_extract_transform(spark, title_principals_data_path)

    # Perform business queries
    query_rabotiahov(title_principals_df)

if __name__ == '__main__':
    main()