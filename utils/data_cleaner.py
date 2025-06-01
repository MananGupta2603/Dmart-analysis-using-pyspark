# utils/data_cleaner.py

from pyspark.sql.functions import col, when, count, lit
from pyspark.sql.types import DoubleType, IntegerType, DateType

# Utility function to show missing values in a DataFrame
# This function counts the number of null values in each column of the DataFrame

def show_missing_values(df, df_name="DataFrame"):
    print(f"\nMissing values in {df_name}:")
    df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()


def clean_customer_df(df):
    # Rename columns for consistency
    df = df.withColumnRenamed("Customer ID", "CustomerID") \
            .withColumnRenamed("Customer Name", "CustomerName") \
              .withColumnRenamed("Postal Code", "PostalCode")
    
    # Cast columns to appropriate types
    
    df = df.withColumn("Age", col("Age").cast(IntegerType())) \
           .withColumn("PostalCode", col("PostalCode").cast(IntegerType()))
    
    # Remove duplicates based on key columns

    df = df.dropDuplicates(["CustomerID"])

    # Drop rows with null values in key columns
    # and fill null values in other columns with defaults

    df = df.dropna(subset=["CustomerID"])
    df = df.withColumn("CustomerName", when(col("CustomerName").isNull(), "Unknown").otherwise(col("CustomerName")))    \
           .withColumn("PostalCode", when(col("PostalCode").isNull(), 0).otherwise(col("PostalCode")))   \
           .withColumn("Segment", when(col("Segment").isNull(), "Unknown").otherwise(col("Segment")))   \
           .withColumn("Region", when(col("Region").isNull(), "Unknown").otherwise(col("Region")))  \
           .withColumn("State", when(col("State").isNull(), "Unknown").otherwise(col("State"))) \
           .withColumn("City", when(col("City").isNull(), "Unknown").otherwise(col("City")))    \
           .withColumn("Country", when(col("Country").isNull(), "Unknown").otherwise(col("Country"))) \
              .withColumn("Age", when(col("Age").isNull(), 0).otherwise(col("Age"))) 
         


    return df

def clean_product_df(df):

    # Rename columns for consistency
    df = df.withColumnRenamed("Product ID", "ProductID")
    df = df.withColumnRenamed("Sub-Category", "SubCategory") \
           .withColumnRenamed("Product Name", "ProductName")
    
    # Cast columns to appropriate types
    
    df = df.dropDuplicates(["ProductID"])

    # Drop rows with null values in key columns
    # and fill null values in other columns with defaults
    
    df = df.dropna(subset=["ProductID"])
    df = df.withColumn("Category", when(col("Category").isNull(), "Unknown").otherwise(col("Category"))) \
           .withColumn("SubCategory", when(col("SubCategory").isNull(), "Unknown").otherwise(col("SubCategory"))) \
           .withColumn("ProductName", when(col("ProductName").isNull(), "Unknown").otherwise(col("ProductName")))

    
    return df

def clean_sales_df(df):
    # Rename columns for consistency
    df = df.withColumnRenamed("Product ID", "ProductID") \
           .withColumnRenamed("Customer ID", "CustomerID") \
           .withColumnRenamed("Order Date", "OrderDate") \
           .withColumnRenamed("Ship Date", "ShipDate") \
              .withColumnRenamed("Ship Mode", "ShipMode") \
              .withColumnRenamed("Order ID", "OrderID") \
                .withColumnRenamed("Order Line", "OrderLine")
    
    # Cast columns to appropriate types
    
    df = df.withColumn("Quantity", col("Quantity").cast(IntegerType())) \
           .withColumn("Discount", col("Discount").cast(DoubleType())) \
           .withColumn("Profit", col("Profit").cast(DoubleType())) \
           .withColumn("Sales", col("Sales").cast(DoubleType())) \
           .withColumn("OrderDate", col("OrderDate").cast(DateType())) \
              .withColumn("ShipDate", col("ShipDate").cast(DateType()))
    
    # Remove duplicates based on key columns
                      
    df = df.dropDuplicates(["OrderID", "ProductID", "CustomerID", "OrderLine"]) 

    # Drop rows with null values in key columns
    # and fill null values in other columns with defaults

    df = df.dropna(subset=["ProductID", "CustomerID","OrderID"])
    df = df.withColumn("ShipMode", when(col("ShipMode").isNull(), "Unknown").otherwise(col("ShipMode"))) \
           .withColumn("Quantity", when(col("Quantity").isNull(), 0).otherwise(col("Quantity"))) \
           .withColumn("Profit", when(col("Profit").isNull(), 0).otherwise(col("Profit"))) \
           .withColumn("Sales", when(col("Sales").isNull(), 0).otherwise(col("Sales"))) \
           .withColumn("Discount", when(col("Discount").isNull(), 0).otherwise(col("Discount"))) \
              .withColumn("OrderDate", when(col("OrderDate").isNull(), lit("9999-01-01")).otherwise(col("OrderDate")).cast(DateType())) \
              .withColumn("ShipDate", when(col("ShipDate").isNull(), lit("9999-01-01")).otherwise(col("ShipDate")).cast(DateType())) \
              .withColumn("OrderLine", when(col("OrderLine").isNull(), '0000').otherwise(col("OrderLine"))) 
      
      
    return df

def join_dataframes(sales, customer, product):

    # Join sales with customer and product dataframes
    sales = sales.join(customer, "CustomerID", "inner") \
                 .join(product, "ProductID", "inner")


    return sales

#--------------new code----------------

# def clean_sales_df(df):
#     df = (df
#         .withColumnRenamed("Product ID", "ProductID")
#         .withColumnRenamed("Customer ID", "CustomerID")
#         .withColumnRenamed("Order Date", "OrderDate")
#         .withColumnRenamed("Ship Date", "ShipDate")
#         .withColumnRenamed("Ship Mode", "ShipMode")
#         .withColumnRenamed("Order ID", "OrderID")
#         .withColumnRenamed("Order Line", "OrderLine")
#     )

#     # Cast numeric / date columns
#     df = (df
#         .withColumn("Quantity", col("Quantity").cast(IntegerType()))
#         .withColumn("Discount", col("Discount").cast(DoubleType()))
#         .withColumn("Profit",   col("Profit").cast(DoubleType()))
#         .withColumn("Sales",    col("Sales").cast(DoubleType()))
#         .withColumn("OrderDate", col("OrderDate").cast(DateType()))
#         .withColumn("ShipDate",  col("ShipDate").cast(DateType()))
#     )

#     # De-dup
#     df = df.dropDuplicates(["OrderID", "ProductID", "CustomerID", "OrderLine"])

#     # Drop critical nulls
#     df = df.dropna(subset=["ProductID", "CustomerID", "OrderID"])

#     # Fill remaining nulls
#     df = (df
#         .withColumn("ShipMode", when(col("ShipMode").isNull(), "Unknown").otherwise(col("ShipMode")))
#         .withColumn("Quantity", when(col("Quantity").isNull(), 0).otherwise(col("Quantity")))
#         .withColumn("Profit",   when(col("Profit").isNull(), 0).otherwise(col("Profit")))
#         .withColumn("Sales",    when(col("Sales").isNull(), 0).otherwise(col("Sales")))
#         .withColumn("Discount", when(col("Discount").isNull(), 0).otherwise(col("Discount")))
#         .withColumn("OrderDate",
#             when(col("OrderDate").isNull(), lit("9999-01-01")).otherwise(col("OrderDate"))
#         ).withColumn("OrderDate", col("OrderDate").cast(DateType()))
#         .withColumn("ShipDate",
#             when(col("ShipDate").isNull(), lit("9999-01-01")).otherwise(col("ShipDate"))
#         ).withColumn("ShipDate", col("ShipDate").cast(DateType()))
#         .withColumn("OrderLine", when(col("OrderLine").isNull(), "0000").otherwise(col("OrderLine")))
#     )
#     return df

