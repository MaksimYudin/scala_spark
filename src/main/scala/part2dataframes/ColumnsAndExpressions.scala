package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, expr, when}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("testApp")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val jsonSchema = StructType(Array(
    StructField("name", StringType),
    StructField("salary", LongType),
    StructField("birthdate", DateType)
  ))

  val jsonDF = spark.read
    .format("json")
    .schema(jsonSchema)
    .option("mode", "failFast")
    .option("path", "files/my.json")
    .load()

  import spark.implicits._
  jsonDF.select(col("name")
    ,$"salary"
  , 'birthdate).distinct()

  val my_salary = col("salary") + 999
  jsonDF.select(
    $"name",
    ($"salary" + 100).as("new_salary"),
    my_salary.as("my_salary"),
    expr("salary + 888").as("new_salary2")
    )
    //.show()

  val exprDF = jsonDF.selectExpr(
    "name",
    "salary + 999"
  )

  val jsonDFWithColumn = jsonDF.withColumn("new_salary", col("salary") + 999)
  //jsonDFWithColumn.show()
  val jsonDFRenamed = jsonDFWithColumn.withColumnRenamed("new_salary", "new salary")
  //jsonDFRenamed.selectExpr("'new salary'").show()

  jsonDF.filter(col("name") === "Justin" and col("salary") === 3500)
  jsonDF.filter("name = 'Justin'")

  val jsonFD1 = jsonDF.select("*")
  val jsonFD2 = jsonDF.select("*")
  val jsonDF3 = jsonFD1.union(jsonFD2)

  val moviesDF = spark.read
    .format("json")
    .option("interSchema", "true")
    .option("mode", "failFast")
    .option("path", "files/movies.json")
    .load()

  val profit = moviesDF.col("US_Gross") + moviesDF.col("Worldwide_Gross")
  moviesDF.select(
    $"Title",
    $"Release_Date",
    $"US_Gross",
    $"Worldwide_Gross",
    $"IMDB_Rating"
    )
    .withColumn("total", when($"US_Gross".isNull, 0).otherwise($"US_Gross")
                                  + when($"Worldwide_Gross".isNull, 0).otherwise($"Worldwide_Gross"))
    .where($"IMDB_Rating" >= 6)
    .orderBy($"IMDB_Rating" desc)
    //.withColumn("profit", col("US_Gross") + col("Worldwide_Gross"))
    .show()
}
