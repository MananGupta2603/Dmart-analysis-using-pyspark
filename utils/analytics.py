# utils/analytics.py

from pyspark.sql.functions import sum, avg, count, col, countDistinct

def total_sales_by_category(df):
    return df.groupBy("Category").agg(sum("Sales").alias("TotalSales"))

def highest_purchasing_customer(df):
    return df.groupBy("CustomerID").count().orderBy("count", ascending=False).limit(1)

def average_discount(df):
    return df.agg(avg("Discount").alias("AverageDiscount"))

def unique_products_by_region(df):
    return df.groupBy("Region").agg(countDistinct("ProductID").alias("UniqueProducts"))

def total_profit_by_state(df):
    return df.groupBy("State").agg(sum("Profit").alias("TotalProfit"))

def top_subcategory_sales(df):
    return df.groupBy("SubCategory").agg(sum("Sales").alias("TotalSales")).orderBy("TotalSales", ascending=False).limit(1)

def average_age_by_segment(df):
    return df.groupBy("Segment").agg(avg("Age").alias("AverageAge"))

def orders_by_shipping_mode(df):
    return df.groupBy("ShipMode").count().withColumnRenamed("count", "TotalOrders")

def total_quantity_by_city(df):
    return df.groupBy("City").agg(sum("Quantity").alias("TotalQuantity"))

def top_segment_by_profit_margin(df):
    return df.groupBy("Segment").agg(sum("Profit").alias("TotalProfit")).orderBy("TotalProfit", ascending=False).limit(1)
