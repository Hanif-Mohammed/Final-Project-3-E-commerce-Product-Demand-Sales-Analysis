# Set storage configs
storage_account_name = "storageecommerce"
storage_account_key = "CLZSIPRieeBvBCfFN8yljWxg9Tp28faTS2nlO9LrBnfA0tFgc5bW0XZKAPFLhO1XZkBymz80mZsq+AStuywHWg=="

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    storage_account_key
)

# Bronze ADLS paths
base = f"wasbs://bronzelayer@{storage_account_name}.blob.core.windows.net"
events_path = f"{base}/events/events.csv"
cat_path = f"{base}/category/category_tree.csv"
prop1_path = f"{base}/item_properties/item_properties_part1.csv"
prop2_path = f"{base}/item_properties/item_properties_part2.csv"

#  Read the files
events_df = spark.read.option("header", True).csv(events_path)
category_df = spark.read.option("header", True).csv(cat_path)
item_props1_df = spark.read.option("header", True).csv(prop1_path)
item_props2_df = spark.read.option("header", True).csv(prop2_path)



from pyspark.sql.functions import col, to_date, count, max

# 1. Combine both item_properties parts
item_props_df = item_props1_df.union(item_props2_df)

# Optional: Remove duplicates
item_props_df = item_props_df.dropDuplicates(["itemid", "property", "timestamp"])

# 2. Filter only transactions from events.csv
transactions_df = events_df.filter(col("event") == "transaction")

# Convert timestamp to date for daily grouping
transactions_df = transactions_df.withColumn("event_date", to_date("timestamp"))

# 3. Extract item-category mapping from item_properties
# Assuming rows where `property == 'categoryid'` contain the actual categoryid in `value`
item_category_df = item_props_df.filter(col("property") == "categoryid") \
    .select(col("itemid"), col("value").alias("categoryid"))

# 4. Join transactions with item-category mapping
tx_item_df = transactions_df.join(item_category_df, on="itemid", how="left")

# 5. Join with category_tree to get full hierarchy (optional)
tx_item_cat_df = tx_item_df.join(category_df, on="categoryid", how="left")

# 6. Group by item and date to get daily demand
daily_demand_df = tx_item_cat_df.groupBy("itemid", "event_date").agg(
    count("*").alias("daily_sales_count"),
    max("categoryid").alias("categoryid")
)

# Show result
daily_demand_df.show(10)

# 7. Save to Silver Zone
silver_path = f"wasbs://silver@{storage_account_name}.blob.core.windows.net/daily_demand/"

daily_demand_df.write.format("delta").mode("overwrite").save(silver_path)




silver_path = f"wasbs://silverlayer@{storage_account_name}.blob.core.windows.net/daily_demand/"

daily_demand_df.write.format("delta").mode("overwrite").save(silver_path)

print(" Silver layer written to ADLS!")
