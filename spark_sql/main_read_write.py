from pyspark.sql import SparkSession
from pyspark.sql.functions import col,
from pysspark.sql.types import IntegerType,StringType,

#Initialize sparksession with hive support
spark = spark.builder.appName("app").config("spark.sql.warehouse.dir","/user/hive/warehouse")\
.config("hive.metastore.uris",f'thrift://192.168.14.120:9083').enableHiveSupport().getOrCreate()

#reading csv with sparksession
df2 = spark.read.csv("/user/hadoop/text.csv").option("header",True).option("delimiter",";")
#reading parquet with sparksession
df = spark.read.parquet("path_to_parquet_file")
#parquet with more options
df = spark.read \
    .format("parquet") \
    .option("header", "true") \
    .option("inferSchema","true") \
    .load("path_to_parquet_file")

#reading avro
df = spark.read.format("avro").option("avroSchema",avro_schema_string).load("path_to_avro_file")

##reading from mongo first method
spark = spark.builder.appName("MongoDBRead").config("spark.mongodb.input.uri","mongodb://localhost/testdb.collection").config("spark.mongodb.output.uri","mongodb://localhost/testdb.collection"df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

##reading from mongodb second method
df = spark.read.format("mongodb").option("uri","mongodb://localhost/testdb.collection").option("collection","collection_name").option("database","mongo_db_name").load()

##first way to read from hive table
df = spark.table("datalake.tabla")
##second form with query
df = spark.sql("select * from datalake.table1")

##reading json file

df = spark.read.json("ruta_archivo")

##lectura json mas completo
df = spark.read.format("json").option("inferSchema","true").option("header","true").load("ruta_archivo")

##reading from json in variable
json_data = [
    {"name": "John", "age": 30, "city": "New York"},
    {"name": "Alice", "age": 25, "city": "San Francisco"},
    {"name": "Bob", "age": 35, "city": "Chicago"}
]
df = spark.createDataFrame(json_data)

##reading from jdbc
#POSTGRE:  jdbc:postgresql://<host>:<port>/<database_name>
#MYSQL: jdbc:mysql://<host>:<port>/<database_name>
#SQLSERVER: jdbc:sqlserver://<host>:<port>;database=<database_name>
#TERADATA: jdbc:teradata://<host>:<port>/database=<database_name>
#ORACLE: jdbc:oracle:thin:@<host>:<port>:<SID>
jdbc_url = "jdbc:postgresql://localhost:5432/mydatabase"
connection_properties = {
    "user": "myuser",
    "password": "mypassword",
    "driver": "org.postgresql.Driver"
}
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "mytable") \
    .option("driver", connection_properties["driver"]) \
    .option("user", connection_properties["user"]) \
    .option("password", connection_properties["password"]) \
    .load()


#reading from elasticsearch
#spark-submit --packages org.elasticsearch:elasticsearch-spark-xx:xx.x.x ...
df = spark.read.format("org.elasticsearch.spark.sql")\
    .option("es.nodes","ELASTICSEARCH_HOST")\
    .option("es.port":"<ELASTICSEARCH_PORT>")\
    .option("es.resource","INDEX_NAME/DOCUMENT_TYPE")\
    .load()

##WRITE##


#writing jdbc
jdbc_url = "jdbc:<DATABASE_TYPE>://<HOST>:<PORT>/<DATABASE_NAME>"
table_name = "<TABLE_NAME>"
connection_properties = {
    "user": "<USERNAME>",
    "password": "<PASSWORD>"
}

df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", table_name) \
    .option("driver", "<JDBC_DRIVER>") \
    .mode("overwrite") \
    .options(**connection_properties) \
    .save()
#writing parquet
output_path = "/path/to/output.parquet"
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("hdfs://192.168.14.120:9000/user/hive/warehouse/datalake/nombreTabla")
##another way of write parquet
df.write.parquet(output_path)


#writing avro
output_path = "/path/to/output.avro"
df.write \
    .format("avro") \
    .mode("overwrite") \
    .save(output_path)
#write mongodb
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])
df.write \
    .format("mongo") \
    .option("uri", "mongodb://<MONGODB_HOST>:<MONGODB_PORT>/<DATABASE_NAME>.<COLLECTION_NAME>") \
    .mode("overwrite") \
    .save()
    
#writing elasticsearch
df.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "<ELASTICSEARCH_HOST>") \
    .option("es.port", "<ELASTICSEARCH_PORT>") \
    .option("es.resource", "<INDEX_NAME>") \
    .mode("overwrite") \
    .save()