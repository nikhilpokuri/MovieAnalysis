import os

class Session:
    def load_data(self, spark):
        file_path = os.path.join(os.path.dirname(__file__), "..", "data")
        movies_path = os.path.join(file_path, "movies.csv")
        ratings_path = os.path.join(file_path, "ratings.csv")
        tags_path = os.path.join(file_path, "tags.csv")
        links_path = os.path.join(file_path, "links.csv")
        
        movies_df = spark.read.csv(movies_path, header=True, inferSchema=True)
        ratings_df = spark.read.csv(ratings_path, header=True, inferSchema=True)
        tags_df = spark.read.csv(tags_path, header=True, inferSchema=True)
        links_df = spark.read.csv(links_path, header=True, inferSchema=True)

        return {"movies_df": movies_df, "ratings_df": ratings_df, 
                "tags_df": tags_df, "links_df": links_df}

# if __name__ == "__main__":
#     from pyspark.sql import SparkSession
#     spark = SparkSession.builder.appName("MovieRecommendations").getOrCreate()
#     df = Session().load_data(spark)
#     # print(df)
#     spark.stop()
