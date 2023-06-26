"""
Based on the assumption that the dataset represents financial bank data, here is an example of the fields that might be present in the DataFrame:

age: The age of the bank customer (numeric)
job: The occupation or job of the customer (string)
marital_status: The marital status of the customer (string)
education: The educational background of the customer (string)
balance: The bank account balance of the customer (numeric)
customer_id: Unique identifier for each customer (numeric)
loan: Indicates whether the customer has a loan or not (string)
default: Indicates whether the customer has defaulted on payments (string)
housing: Indicates whether the customer has a housing loan (string)
contact: The type of communication contact with the customer (string)
duration: The duration of the last contact in seconds (numeric)
campaign: The number of contacts performed during the campaign for this customer (numeric)
previous: The number of contacts performed before this campaign for this customer (numeric)
poutcome: The outcome of the previous marketing campaign (string)
subscribed: Indicates whether the customer subscribed to a term deposit (string)
"""
from shlex import join
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,count,sum,avg,max
#Create a SparkSession
spark = SparkSession.builder \
    .appName("Bank data operations") \
    .gerOrCreate()

#Load the bank data from CSV
bank_data = spark.read \
    .format("csv") \
    .option("header","true") \
    .option("inferSchema","true") \
    .load("/path/to/bank_data.csv")

#Display the schema and sample records
bank_data.printSchema()
bank_data.show(5,truncate=False)

#Select specific columns
selected_columns = bank_data.select("age","job","balance")
selected_columns.show(5)

#Filter records based on a condition
filtered_data = bank_data.filter(col("age") >= 30)
filtered_data.show(5)

#Group data by a column and perform aggregations
grouped_data = bank_data.groupBy("education").agg(count("age").alias("count"),sum("balance").alias("total_balance"))
grouped_data.show()

#Apply a having clause on aggregated data
having_data = grouped_data.filter(col("count") > 100)
having_data.show()

#Perform various aggregations
aggregated_data = bank_data.agg(
    count("*").alias("total_records"),
    max("age").alias("max_age"),
    avg("balance").alias("avg_balance")
)
aggregated_data.show()

#Join with another DataFrame
customers = spark.read.format("csv").option("header","true").load("/path/to/customers.csv")
joined_data = bank_data.join(customers,"customers_id","inner")
joined_data.show(5)

#Parse data and transform columns
transformed_data = bank_data.withColumn("parsed_balance",col("balance").cast("float"))
transformed_data.show(5)
