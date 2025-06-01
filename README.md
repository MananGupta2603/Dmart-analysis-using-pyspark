
# 🛒 Dmart Analysis Using PySpark

This project performs analytical insights on Dmart sales, customer, and product data using Apache Spark and PySpark. It loads CSV data, cleans and transforms it, and performs queries to extract business metrics.

## 📁 Project Structure

```
Dmart-analysis-using-pyspark/
│
├── data/
│   ├── Customer.csv
│   ├── Product.csv
│   └── Sales.csv
│
├── utils/
│   ├── data\_loader.py
│   ├── data\_cleaner.py
│   └── analytics.py
│
├── dmart\_analysis.py
└── README.md
```


## 🚀 Features

- Load and preview data using Spark DataFrames
- Clean missing or inconsistent data
- Join customer, product, and sales datasets
- Perform business analytics:
  - Total sales by category
  - Top purchasing customers
  - Discounts and profit margins
  - Quantity by city, region-based metrics, and more

## 📦 Requirements

- Python 3.7+
- Java JDK 17+ (not JDK 8)
- Apache Spark 3.x
- PySpark

## 🛠️ Setup Instructions

1. **Install Java (JDK 17 or newer)**  
   Download from [Oracle](https://www.oracle.com/java/technologies/javase-jdk17-downloads.html) or use OpenJDK.

   Set environment variable:
   ```bash
   export JAVA_HOME="path_to_your_jdk"


2. **Install Python Packages**

   ```bash
   pip install pyspark
   ```

3. **Clone this repository**

   ```bash
   git clone https://github.com/your-username/Dmart-analysis-using-pyspark.git
   cd Dmart-analysis-using-pyspark
   ```

4. **Run the script**

   ```bash
   python dmart_analysis.py
   ```

## 🧪 Sample Output

Sample output printed to the terminal includes:

* Schema and preview of input data
* Cleaned and joined dataset
* Results for each analysis query:

  * Example:

    ```
    Top Segment by Profit Margin:
    +----------+------------+
    | Segment  | TotalProfit|
    +----------+------------+
    | Consumer | 134119.20  |
    +----------+------------+
    ```

## 📌 Notes

* Make sure `JAVA_HOME` is correctly set. Spark needs it to start the JVM.
* On Windows, you may see PID-related messages if Spark isn't stopped — use `spark.stop()` at the end.
* Warnings about `winutils.exe` or `HADOOP_HOME` can be ignored unless you're using Hadoop features.

