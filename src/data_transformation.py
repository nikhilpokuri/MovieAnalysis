from pyspark.sql import functions as f
class TransformData:
    def transform_data(self, datasets):
        ratings_df = datasets.get("ratings_df")
        
        # Converting timestamp column to a readable format
        ratings_df = ratings_df.withColumn("timestamp", f.from_unixtime(f.col("timestamp"))).withColumn(
            "timestamp", f.date_format("timestamp", "dd-MM-yyyy HH:mm:ss")
        )
        
        datasets["ratings_df"] = ratings_df
        return datasets

# if __name__ == "__main__":
#     from data_ingestion import Session
#     from pyspark.sql import SparkSession
#     spark = SparkSession.builder.appName("MovieTrendsAnalysis").getOrCreate()
#     datasets = Session().load_data(spark)
#     transformed_df = TransformData().transform_data(datasets)
#     transformed_df.show()
#     spark.stop()