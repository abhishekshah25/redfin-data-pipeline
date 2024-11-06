from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("RedFinDataPipeline").getOrCreate()

def transform_date():
    raw_data_s3_bucket = "s3://redfin-data-yml/store-raw-data-yml/city_market_tracker.tsv000.gz"
    transform_data_s3_bucket = "s3://redfin-data-yml/redfin-transform-zone-yml/redfin_data.parquet"
    redfin_data = spark.read.csv(raw_data_s3_bucket, header=True, inferSchema=True, sep= "\t")

    df_redfin = redfin_data.select(['period_end','period_duration', 'city', 'state', 'property_type',
        'median_sale_price', 'median_ppsf', 'homes_sold', 'inventory', 'months_of_supply', 'median_dom', 'sold_above_list', 'last_updated'])
    df_redfin = df_redfin.na.drop()
    df_redfin = df_redfin.withColumn("period_end_yr", year(col("period_end")))
    df_redfin = df_redfin.withColumn("period_end_month", month(col("period_end")))
    df_redfin = df_redfin.drop("period_end", "last_updated")
    df_redfin = df_redfin.withColumn("period_end_month", 
                     when(col("period_end_month") == 1, "January")
                    .when(col("period_end_month") == 2, "February")
                    .when(col("period_end_month") == 3, "March")
                    .when(col("period_end_month") == 4, "April")
                    .when(col("period_end_month") == 5, "May")
                    .when(col("period_end_month") == 6, "June")
                    .when(col("period_end_month") == 7, "July")
                    .when(col("period_end_month") == 8, "August")
                    .when(col("period_end_month") == 9, "September")
                    .when(col("period_end_month") == 10, "October")
                    .when(col("period_end_month") == 11, "November")
                    .when(col("period_end_month") == 12, "December")
                    .otherwise("Unknown")
                    )
    df_redfin.write.mode("overwrite").parquet(transform_data_s3_bucket)

transform_date()
