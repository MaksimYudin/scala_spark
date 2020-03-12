package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object DataFrame extends App{
  val spark = SparkSession.builder()
    .appName("testApp")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val phoneData = Seq(
    ("Nokia", 2000, "USA", 1500),
    ("HTC", 2500, "Korea", 2000),
    ("Sumsung", 3000, "Korea", 3000),
    ("Apple", 2000, "USA", 5000)
  )

  import spark.implicits._
  val myDF = phoneData.toDF("model", "battery", "country", "price")
  myDF.show()
  myDF.printSchema()

  val jsonSchema = StructType(Array(
    StructField("name", StringType),
    StructField("salary", LongType)
  ))


  val jsonDF = spark.read
      .format("json")
      .schema(jsonSchema)
      .load("files/my.json")


}
