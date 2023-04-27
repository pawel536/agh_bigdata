# from pyspark.shell import sc
# from read import readpy
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext, SparkFiles
from pyspark.sql import functions as F


# .config("spark.some.config.option", "some-value") \

def readpy(path):
    def readdata(path):
        isDataR = isDataSchemaReadable()
        if (isDataR):
            return returnreaddata(path)

    def isDataAvailable():
        return True

    def isDataSchemaReadable():
        return True

    def returnreaddata(path):
        dfout = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(path)
        return dfout

    return readdata(path)


def writepy(df2):
    df2.write.option("header", True).format("csv").mode('overwrite').save("./chem_new.csv")


def modpy(data):
    data = data.withColumn("CRSQ", F.col("Cr (mg/kg)") ** 2)  # data.na.fill({"Country" : "Missing", "CustomerID" :
    # 987654 })
    return data


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local") \
        .appName("Nazwa aplikacji") \
        .getOrCreate()

    # sc = spark.Spark)
    # sc.addPyFile("venv/read.py")

    df = readpy("chem.csv")
    df.printSchema()
    df = modpy(df)
    writepy(df)

# ############# TO DO ######################
# DO OSOBNYCH PLIKÓW
# JARY ???
# POPRAWA ZAPISU CSV
