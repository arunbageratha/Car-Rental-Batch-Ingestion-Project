from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, round, datediff
import argparse

def process_car_rental_data(data_date):

    # Initialize SparkSession and attach required Snowflake jars (Dataproc picks from GCS)
    # spark = SparkSession.builder \
    #     .appName("SnowflakeDataRead") \
    #     .config("spark.jars", "gs://snowflake-projects-test-gds/snowflake_jars/spark-snowflake_2.12-2.15.0-spark_3.4.jar,gs://snowflake-projects-test-gds/snowflake_jars/snowflake-jdbc-3.16.0.jar") \
    #     .getOrCreate()

    spark = SparkSession.builder \
        .appName("CarRentalDataProcessing") \
        .getOrCreate()


    # Define GCS file path based on the execution date argument (yyyymmdd)
    gcs_file_path = f"gs://snowflake-projects-test-gds/car_rental_data/car_rental_daily_data/car_rental_{data_date}.json"

    # Read raw JSON data (multiline JSON array)
    raw_df = spark.read.option("multiline", "true").json(gcs_file_path)

    # Data validation: filter out rows missing mandatory fields
    validated_df = raw_df.filter(
        col("rental_id").isNotNull() & 
        col("customer_id").isNotNull() & 
        col("car.make").isNotNull() & 
        col("car.model").isNotNull() & 
        col("car.year").isNotNull() & 
        col("rental_period.start_date").isNotNull() & 
        col("rental_period.end_date").isNotNull() & 
        col("rental_location.pickup_location").isNotNull() & 
        col("rental_location.dropoff_location").isNotNull() & 
        col("amount").isNotNull() & 
        col("quantity").isNotNull()
    )

    # Transformation 1: Calculate rental duration in days    
    transformed_df = validated_df.withColumn(
        "rental_duration_days", 
        datediff(col("rental_period.end_date"), col("rental_period.start_date"))
    )
    # Transformation 2: Derive additional quantitative attributes
    transformed_df = transformed_df.withColumn(
        "total_rental_amount", 
        col("amount") * col("quantity")
    ).withColumn(
        "average_daily_rental_amount", 
        round(col("total_rental_amount") / col("rental_duration_days"), 2)
    ).withColumn(
        "is_long_rental", 
        when(col("rental_duration_days") > 7, lit(1)).otherwise(lit(0))
    )

    # Read dimension tables from Snowflake (via Spark connector)
    snowflake_options = {
        "sfURL": "https://er456-wb22964.snowflakecomputing.com",
        "sfAccount": "PGFPLNN-WB22964",
        "sfUser": "DFGTYRFF",
        "sfPassword": "fggfdf",
        "sfDatabase": "car_rental",
        "sfSchema": "PUBLIC",
        "sfWarehouse": "COMPUTE_WH",
        "sfRole": "ACCOUNTADMIN"
    }

    # Source name alias for Snowflake Spark connector
    SNOWFLAKE_SOURCE_NAME = "snowflake"

    # Dimension loads
    car_dim_df = spark.read \
        .format(SNOWFLAKE_SOURCE_NAME) \
        .options(**snowflake_options) \
        .option("dbtable", "car_dim") \
        .load()

    location_dim_df = spark.read \
        .format(SNOWFLAKE_SOURCE_NAME) \
        .options(**snowflake_options) \
        .option("dbtable", "location_dim") \
        .load()

    date_dim_df = spark.read \
        .format(SNOWFLAKE_SOURCE_NAME) \
        .options(**snowflake_options) \
        .option("dbtable", "date_dim") \
        .load()

    customer_dim_df = spark.read \
        .format(SNOWFLAKE_SOURCE_NAME) \
        .options(**snowflake_options) \
        .option("dbtable", "customer_dim") \
        .load()

    # Join raw data with dimension tables to derive surrogate keys
    # 1) car_dim
    fact_df = transformed_df.alias("raw") \
        .join(car_dim_df.alias("car"), 
              (col("raw.car.make") == col("car.make")) & 
              (col("raw.car.model") == col("car.model")) & 
              (col("raw.car.year") == col("car.year"))
        ) \
        .select(
            col("raw.rental_id"),
            col("raw.customer_id"),
            col("car.car_key"),
            col("raw.rental_location.pickup_location").alias("pickup_location"),
            col("raw.rental_location.dropoff_location").alias("dropoff_location"),
            col("raw.rental_period.start_date").alias("start_date"),
            col("raw.rental_period.end_date").alias("end_date"),
            col("raw.amount"),
            col("raw.quantity"),
            col("raw.rental_duration_days"),
            col("raw.total_rental_amount"),
            col("raw.average_daily_rental_amount"),
            col("raw.is_long_rental")
        )

    # 2) location_dim for pickup_location
    fact_df = fact_df.alias("fact") \
        .join(location_dim_df.alias("pickup_loc"), col("fact.pickup_location") == col("pickup_loc.location_name"), "left") \
        .withColumnRenamed("location_key", "pickup_location_key") \
        .drop("pickup_location")

    # 3) location_dim for dropoff_location
    fact_df = fact_df.alias("fact") \
        .join(location_dim_df.alias("dropoff_loc"), col("fact.dropoff_location") == col("dropoff_loc.location_name"), "left") \
        .withColumnRenamed("location_key", "dropoff_location_key") \
        .drop("dropoff_location")

    # 4) date_dim for start_date
    fact_df = fact_df.alias("fact") \
        .join(date_dim_df.alias("start_date_dim"), col("fact.start_date") == col("start_date_dim.date"), "left") \
        .withColumnRenamed("date_key", "start_date_key") \
        .drop("start_date")

    # 5) date_dim for end_date
    fact_df = fact_df.alias("fact") \
        .join(date_dim_df.alias("end_date_dim"), col("fact.end_date") == col("end_date_dim.date"), "left") \
        .withColumnRenamed("date_key", "end_date_key") \
        .drop("end_date")

    # 6) customer_dim to get customer_key (Note: could be refined to pick is_current=true row)
    fact_df = fact_df.alias("fact") \
        .join(customer_dim_df.alias("cust"), col("fact.customer_id") == col("cust.customer_id"), "left") \
        .withColumnRenamed("customer_key", "customer_key") \
        .drop("customer_id")

    # Final projection: columns for fact table load
    fact_df = fact_df.select(
        "rental_id",
        "customer_key",
        "car_key",
        "pickup_location_key",
        "dropoff_location_key",
        "start_date_key",
        "end_date_key",
        "amount",
        "quantity",
        "rental_duration_days",
        "total_rental_amount",
        "average_daily_rental_amount",
        "is_long_rental"
    )

    # Write fact records to Snowflake (append mode)
    fact_df.write \
        .format(SNOWFLAKE_SOURCE_NAME) \
        .options(**snowflake_options) \
        .option("dbtable", "rentals_fact") \
        .mode("append") \
        .save()

if __name__ == "__main__":
    # CLI: expects --date yyyymmdd
    parser = argparse.ArgumentParser(description='Process date argument')
    parser.add_argument('--date', type=str, required=True, help='Date in yyyymmdd format')
    args = parser.parse_args()
     
    process_car_rental_data(args.date)

# Example: process_car_rental_data("20250903")
