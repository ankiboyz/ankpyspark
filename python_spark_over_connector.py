from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.\
        builder.\
        appName("pyspark-notebook2").\
        master("spark://192.168.2.221:7077").\
        config("spark.executor.memory", "1g").\
        config("spark.mongodb.input.uri","mongodb://DUP_INVOICE:green123@192.168.2.193:27017/?authSource=BUSINESS_CONTROLS").\
        config("spark.mongodb.input.database","BUSINESS_CONTROLS").\
        config("spark.mongodb.input.collection","DUPLICATE_INVOICES").\
        config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.2").\
        getOrCreate()

# master("local[*]")
# schema = StructType([StructField("name", StringType()),
#                      StructField("age", IntegerType()),
#                      StructField("sex", StringType())])



# df=spark.createDataFrame([('caocao',36,'male'),('sunqun',26,'male')], schema)
# df.show()
# df.write.format('com.mongodb.spark.sql.DefaultSource').mode("append").save()
# spark.stop()
#reading dataframes from MongoDB

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
# df.printSchema()
# df.show()

df.createOrReplaceTempView("tempDuplicateInvoices")
# newdf = spark.sql("""select ID FROM tempDuplicateInvoices""")\
# newdf = spark.sql("""select ID,tags FROM tempDuplicateInvoices where array_contains(categories, 'SC1')""")\
# newdf = spark.sql("""select ID['HDLR1']['WRBTR'].cast("decimal") AS sumOf FROM tempDuplicateInvoices""")\
# newdf = spark.sql("""select cast(ID['HDLR1']['WRBTR'] as decimal) AS sumOf FROM tempDuplicateInvoices""")\
newdf = spark.sql("""select sum(cast(ID["HDRL1"]["WRBTR"] as decimal(30,5))) AS sumOf FROM tempDuplicateInvoices""")\


newdf.show()
# master("spark://192.168.2.221:7077").\

# config("spark.mongodb.output.uri","mongodb://mongo1:27017,mongo2:27018,mongo3:27019/Stocks.Source?replicaSet=rs0").\
