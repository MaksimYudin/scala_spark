package part2dataframes



import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSource extends App{

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

  val jsonDF2 = spark.read
    .format("json")
    .schema(jsonSchema)
    .option("mode", "failFast")
    .option("path", "files/my.json")
    .load()

  val jsonDF3 = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "files/my.json",
      "inferSchema" -> "true"
    ))
    .load()
  //jsonDF3.show()

  jsonDF3.write
    //.format("json")
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("path", "result/my.csv")
    .save()

  val jsonDF4 = spark.read
      .format("json")
      .option("dateFormat", "YYYY-MM-dd")
      .option("allowSingleQuotes", "true")
      .option("compression", "uncompressed")
      .json("files/my.json")

  val csvSchema = StructType(Array(
    StructField("date", DateType),
    StructField("name", StringType),
    StructField("price", LongType)
  ))

  val csvDF = spark.read
    .option("dateFormat", "YYYY-MM-dd")
    .schema(csvSchema)
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("files/my.csv")

  csvDF.write
    .mode(SaveMode.Overwrite)
    .save("files/my.parquet")

  val jdbcDF = spark.read
    .format("jdbc")
    .option("driver", "org.sqlite.JDBC")
    .option("url", "jdbc:sqlite:file:/home/maksim/Documents/java_projects/TimeControl/TimeControl.sqlite")
    .option("dbtable", "student")
    .option("user", "")
    .option("password", "")
    .load()

  csvDF.write
    .format("jdbc")
    .option("driver", "org.sqlite.JDBC")
    .option("url", "jdbc:sqlite:file:/home/maksim/Documents/java_projects/TimeControl/TimeControl.sqlite")
    .option("dbtable", "csv_test")
    .option("user", "")
    .option("password", "")
    .save()
}
