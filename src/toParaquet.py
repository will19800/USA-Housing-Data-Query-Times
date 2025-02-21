from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("csvToParaquet").getOrCreate()

# Convert CSV to data frame
realEstateDF = spark.read.csv("/Users/willchen/Documents/usaRealEstateData/realtor-data.zip.csv", header=True, inferSchema=True)

# Convert CSV to Parquet format
realEstateParaquet = realEstateDF
realEstateParaquet.write.parquet("/Users/willchen/Documents/usaRealEstateData/realtor-data.paraquet")

