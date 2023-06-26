from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Bank Data Join") \
    .getOrCreate()

# Create a DataFrame with additional financial information
financial_data = spark.createDataFrame([
    (1, "ABC Bank", "Credit Card"),
    (2, "XYZ Bank", "Mortgage"),
    (3, "DEF Bank", "Savings Account")
], ["customer_id", "bank_name", "product"])

# Display the schema and sample records of financial data
financial_data.printSchema()
financial_data.show()

bank_data = spark.read.format("parquet") \
    .option("inferSchema","true") \
    .load("<ruta_archivos>")

# Perform an inner join with the bank data DataFrame
inner_join_data = bank_data.join(financial_data, "customer_id", "inner")
inner_join_data.show(10)

# Perform a left join with the bank data DataFrame
left_join_data = bank_data.join(financial_data, "customer_id", "left")
left_join_data.show(10)

# Perform a right join with the bank data DataFrame
right_join_data = bank_data.join(financial_data, "customer_id", "right")
right_join_data.show(10)

# Close the SparkSession
spark.stop()
