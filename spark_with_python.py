# spark_with_python.py
from pyspark.sql import SparkSession
import wget

# spark = SparkSession.builder.master("local").getOrCreate()
# url = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
# wget.download(url)
spark = SparkSession.builder.master("spark://192.168.2.221:7077").getOrCreate()
# spark = SparkSession.builder.master("local").getOrCreate()


url = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
wget.download(url)
data = spark.read.csv("/Ank-Projects/ankpyspark/iris.data")
data.show(n=5)