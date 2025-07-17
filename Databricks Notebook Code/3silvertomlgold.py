#simple moving average

from pyspark.sql.functions import col, avg, lit
import datetime

# Step 1: Config
storage_account_name = "storageecommerce"
storage_account_key = "CLZSIPRieeBvBCfFN8yljWxg9Tp28faTS2nlO9LrBnfA0tFgc5bW0XZKAPFLhO1XZkBymz80mZsq+AStuywHWg=="
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    storage_account_key
)

silver_path = f"wasbs://silverlayer@{storage_account_name}.blob.core.windows.net/daily_demand/"
gold_path   = f"wasbs://goldlayer@{storage_account_name}.blob.core.windows.net/simple_7day_forecast/"

# Step 2: Load data
df = spark.read.format("delta").load(silver_path).select("itemid", "event_date", "daily_sales_count")

# Step 3: Get top 5 items (any frequency)
top_items = df.groupBy("itemid").count().orderBy(col("count").desc()).limit(5) \
    .rdd.flatMap(lambda x: [x["itemid"]]).collect()

# Step 4: Calculate average daily sales for each item
avg_df = df.filter(col("itemid").isin(top_items)) \
    .groupBy("itemid") \
    .agg(avg("daily_sales_count").alias("yhat"))

# Step 5: Generate next 7 days for each item
today = datetime.date.today()
future_rows = []

for row in avg_df.collect():
    item = row["itemid"]
    yhat = row["yhat"]
    for i in range(1, 8):
        forecast_date = today + datetime.timedelta(days=i)
        future_rows.append((item, forecast_date.strftime('%Y-%m-%d'), yhat))

# Step 6: Create DataFrame and save
future_df = spark.createDataFrame(future_rows, ["itemid", "ds", "yhat"])
future_df.write.format("delta").mode("overwrite").save(gold_path)

print(" 7-day naive forecast saved to Gold layer.")



forecast_df = spark.read.format("delta").load(gold_path)
display(forecast_df)




# ARIMA Model

from pyspark.sql.functions import col, to_date, sum as spark_sum, count as spark_count
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
import datetime


# Azure Storage Config

storage_account_name = "storageecommerce"
storage_account_key = "CLZSIPRieeBvBCfFN8yljWxg9Tp28faTS2nlO9LrBnfA0tFgc5bW0XZKAPFLhO1XZkBymz80mZsq+AStuywHWg=="

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    storage_account_key
)


# Define Paths

silver_path       = f"wasbs://silverlayer@{storage_account_name}.blob.core.windows.net/daily_demand/"
gold_path         = f"wasbs://goldlayer@{storage_account_name}.blob.core.windows.net/arima_item_forecast/"
gold_csv_path     = f"wasbs://goldlayer@{storage_account_name}.blob.core.windows.net/arima_item_forecast_csv/"


# Load Silver Layer

df = spark.read.format("delta").load(silver_path).select("itemid", "event_date", "daily_sales_count")


# Daily sales per item per day

daily_df = df.groupBy("event_date", "itemid") \
    .agg(spark_sum("daily_sales_count").alias("y")) \
    .withColumn("ds", to_date("event_date")) \
    .select("itemid", "ds", "y") \
    .withColumnRenamed("itemid", "group")


#  Find top 5 most active items

item_day_counts = daily_df.groupBy("group").agg(spark_count("ds").alias("active_days"))
top_items = item_day_counts.orderBy(col("active_days").desc()).limit(5).rdd.map(lambda row: row["group"]).collect()
print(" Top active itemids:", top_items)


# Filter for top items

filtered_df = daily_df.filter(col("group").isin(top_items))

# Define ARIMA forecast schema

from pyspark.sql.types import TimestampType

schema = StructType([
    StructField("group", StringType(), True),
    StructField("ds", TimestampType(), True),
    StructField("yhat", DoubleType(), True)
])



# ARIMA Forecast Function (with MLflow)

def arima_forecast(pdf: pd.DataFrame) -> pd.DataFrame:
    import mlflow
    import mlflow.sklearn

    group = str(pdf.iloc[0]["group"])
    try:
        pdf['ds'] = pd.to_datetime(pdf['ds'])
        ts = pdf.sort_values("ds").set_index("ds")["y"]

        # Train ARIMA
        model = ARIMA(ts, order=(1, 1, 1))
        model_fit = model.fit()
        forecast = model_fit.forecast(steps=7)

        last_date = ts.index[-1]
        future_dates = [last_date + pd.Timedelta(days=i) for i in range(1, 8)]

        result = pd.DataFrame({
            "group": [group] * 7,
            "ds": future_dates,
            "yhat": forecast.values
        })

        #  MLflow Tracking
        with mlflow.start_run(run_name=f"ARIMA_{group}"):
            mlflow.log_param("model", "ARIMA")
            mlflow.log_param("order", "(1,1,1)")
            mlflow.log_param("group", group)
            mlflow.log_metric("train_count", len(ts))
            mlflow.sklearn.log_model(model_fit, f"model_arima_{group}")
            print(f" Model for item {group} logged to MLflow.")

        return result

    except Exception as e:
        print(f" ARIMA failed for item {group}: {e}")
        return pd.DataFrame(columns=["group", "ds", "yhat"])

# Apply ARIMA per item

forecast_df = filtered_df.groupBy("group").applyInPandas(arima_forecast, schema=schema)


# Final cast for ds column

forecast_df = forecast_df.withColumn("ds", col("ds").cast("date"))


# Save to Gold Layer (Delta + CSV)

forecast_df.write.format("delta").mode("overwrite").save(gold_path)
forecast_df.write.option("header", "true").mode("overwrite").csv(gold_csv_path)

print(" ARIMA item-level forecast saved to Gold Layer.")
forecast_df.show()




