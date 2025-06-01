# dmart_analysis.py

from utils.data_loader import load_csv_to_df
from utils.data_cleaner import clean_customer_df, clean_product_df, clean_sales_df, join_dataframes
from utils.analytics import (
    total_sales_by_category,
    highest_purchasing_customer,
    average_discount,
    unique_products_by_region,
    total_profit_by_state,
    top_subcategory_sales,
    average_age_by_segment,
    orders_by_shipping_mode,
    total_quantity_by_city,
    top_segment_by_profit_margin
)
from pyspark.sql import SparkSession

def create_spark_session(app_name="DmartAnalysis"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def main():
    spark = create_spark_session()

    # Load data
    customer_df = load_csv_to_df(spark, "Dmart-analysis-using-pyspark\data\Customer.csv")
    print("Customer DF Schema:")
    customer_df.printSchema()
    print("Customer DF Preview:")
    customer_df.show(5)

    product_df = load_csv_to_df(spark, "Dmart-analysis-using-pyspark\data\Product.csv")
    print("Product DF Schema:")
    product_df.printSchema()
    print("Product DF Preview:")
    product_df.show(5)

    sales_df = load_csv_to_df(spark, "Dmart-analysis-using-pyspark\data\Sales.csv")
    print("Sales DF Schema:")
    sales_df.printSchema()
    print("Sales DF Preview:")
    sales_df.show(5)

    # Clean and transform data
    customer_df = clean_customer_df(customer_df)
    product_df = clean_product_df(product_df)
    sales_df = clean_sales_df(sales_df)

    # Join data
    joined_df = join_dataframes(sales_df, customer_df, product_df)

    while True:
        print("\nSelect an analysis to run (1-10), or type 0 to exit:")
        print("1. Total Sales by Category")
        print("2. Highest Purchasing Customer")
        print("3. Average Discount")
        print("4. Unique Products Sold by Region")
        print("5. Total Profit by State")
        print("6. Top Sub-category by Sales")
        print("7. Average Age by Segment")
        print("8. Orders by Shipping Mode")
        print("9. Total Quantity by City")
        print("10. Top Segment by Profit Margin")

        choice = input("Enter your choice: ")

        if choice == "0":
            print("Exiting...")
            break
        elif choice == "1":
            print("Total Sales by Category:")
            total_sales_by_category(joined_df).show()
        elif choice == "2":
            print("Highest Purchasing Customer:")
            highest_purchasing_customer(joined_df).show()
        elif choice == "3":
            print("Average Discount:")
            average_discount(joined_df).show()
        elif choice == "4":
            print("Unique Products Sold by Region:")
            unique_products_by_region(joined_df).show()
        elif choice == "5":
            print("Total Profit by State:")
            total_profit_by_state(joined_df).show()
        elif choice == "6":
            print("Top Sub-category by Sales:")
            top_subcategory_sales(joined_df).show()
        elif choice == "7":
            print("Average Age by Segment:")
            average_age_by_segment(joined_df).show()
        elif choice == "8":
            print("Orders by Shipping Mode:")
            orders_by_shipping_mode(joined_df).show()
        elif choice == "9":
            print("Total Quantity by City:")
            total_quantity_by_city(joined_df).show()
        elif choice == "10":
            print("Top Segment by Profit Margin:")
            top_segment_by_profit_margin(joined_df).show()
        else:
            print("Invalid choice. Please enter a number between 0 and 10.")
    
    spark.stop()

if __name__ == "__main__":
    main()
