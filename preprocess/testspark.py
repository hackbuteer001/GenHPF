from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[1]")
    .config("spark.driver.host", "localhost")
    .config("spark.driver.memory", "100g")
    .config("spark.driver.maxResultSize", "10g")
    .config("spark.network.timeout", "100s")
    .appName("Main_Preprocess")
    .getOrCreate()
)

# spark = SparkSession.builder.appName('Myapp').master('local').getOrCreate()

df = spark.read.csv('/home/caoyu/data/eicu/patient.csv',header=True)

df.printSchema()