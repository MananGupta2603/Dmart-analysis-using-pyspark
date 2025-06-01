# utils/data_cleaner.py

from pyspark.sql.functions import col

def clean_customer_df(df):
    df = df.withColumnRenamed("Customer ID", "CustomerID")
    df = df.withColumnRenamed("Customer Name", "CustomerName")

    df = df.dropna(subset=["CustomerID"])
    return df

def clean_product_df(df):
    df = df.withColumnRenamed("Product ID", "ProductID")
    df = df.withColumnRenamed("Sub-Category", "SubCategory") \
           .withColumnRenamed("Product Name", "ProductName")
    return df

def clean_sales_df(df):
    df = df.withColumnRenamed("Product ID", "ProductID") \
           .withColumnRenamed("Customer ID", "CustomerID")
    df = df.withColumnRenamed("Order Date", "OrderDate") \
           .withColumnRenamed("Ship Date", "ShipDate") \
              .withColumnRenamed("Ship Mode", "ShipMode") \
              .withColumnRenamed("Order ID", "OrderID") \
                .withColumnRenamed("Order Line", "OrderLine")
              
           
                
    return df

def join_dataframes(sales, customer, product):
    sales = sales.join(customer, "CustomerID", "inner")
    sales = sales.join(product, "ProductID", "inner")
    return sales
