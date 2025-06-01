# utils/data_loader.py

def load_csv_to_df(spark, file_path):
    return spark.read.option("header", "true").csv(file_path)
