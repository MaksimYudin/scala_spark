package com.example.scala_test

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, when, _}
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Use this to test the app locally, from sbt:
  * sbt "run inputFile.txt outputFile.txt"
  *  (+ select CountingLocalApp when prompted)
  */
object CountingLocalApp extends App {
  //val (inputFile, outputFile) = (args(0), args(1))
  /*
  val name1: String = "files/1.txt"
  val name2: String = "result"
  val (inputFile, outputFile) = (name1, name2)

val conf = new SparkConf()
    .setMaster("local")
    .setAppName("my awesome app")
  Runner.run(conf, inputFile, outputFile)
  */


  //val add_01: String = (x: String) => x + "-01"

  //  val df = MyTestSpark.readCSVToDataFarme(spark)
  //  df.withColumn("new_col", concat(col("year_month"), lit("-01")))
  //    .show(10)
  //
  //  val df2 = df.withColumnRenamed("year_month", "new_col1")
  //  //df2.printSchema()
  //
  //  val schema2 = new StructType()
  //    .add("year", StringType)
  //    .add("month", StringType)
  //    .add("type", StringType)
  //
  //  df.select()

  //testRenameDF()

  //testCaseWhenDF()

  //testPivotDF()

  //testStructure()

  //testFilter()

  //testJoin()

  //testDate()

  //testAgg()

  testWindowFunc()

  def testRenameDF(): Unit = {

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    spark.sparkContext.setLogLevel("ERROR")

    val data = Seq(Row(Row("James ", "", "Smith"), "36636", "M", 3000),
      Row(Row("Michael ", "Rose", ""), "40288", "M", 4000),
      Row(Row("Robert ", "", "Williams"), "42114", "M", 4000),
      Row(Row("Maria ", "Anne", "Jones"), "39192", "F", 4000),
      Row(Row("Jen", "Mary", "Brown"), "", "F", -1)
    )

    val schema = new StructType()
      .add("name", new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("lastname", StringType))
      .add("dob", StringType)
      .add("gender", StringType)
      .add("salary", IntegerType)

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    //df.printSchema()

    //df.select(col("name.firstname").as("fname")).show()

    val df4 = df.withColumn("fname", col("name.firstname"))
      .withColumn("mname", col("name.middlename"))
      .withColumn("lname", col("name.lastname"))
      .drop("name")
    df4.printSchema()

    val old_columns = Seq("dob", "gender", "salary", "fname", "mname", "lname")
    val new_columns = Seq("DateOfBirth", "Sex", "salary", "firstName", "middleName", "lastName")
    val columnsList = old_columns.zip(new_columns).map(f => {
      col(f._1).as(f._2)
    })
    val df5 = df4.select(columnsList: _*)
    df5.printSchema()
  }

  def testCaseWhenDF(): Unit = {

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    spark.sparkContext.setLogLevel("ERROR")

    val data = List(("James", "", "Smith", "36636", "M", 60000),
      ("Michael", "Rose", "", "40288", "M", 70000),
      ("Robert", "", "Williams", "42114", "", 400000),
      ("Maria", "Anne", "Jones", "39192", "F", 500000),
      ("Jen", "Mary", "Brown", "", "F", 0))

    val cols = Seq("first_name", "middle_name", "last_name", "dob", "gender", "salary")
    val df = spark.createDataFrame(data).toDF(cols: _*)

    val df2 = df.withColumn("new_gender",
      when(col("gender") === "M", "Male")
    .when(col("gender") === "F", "Female")
    .otherwise("None"))

    val df4 = df.select(col("*"),
      when(col("gender") === "M", "Male")
        .when(col("gender") === "F", "Female")
        .otherwise("None").alias("sex"), lit("123").alias("my_field"))
    df4.show()

    val df5 = df.withColumn("new_gender",
      expr("case when gender = 'M' then 'Male' " +
        "when gender = 'F' then 'Female' " +
        "else 'Unknown' end"))
    df5.show()
  }

  def testPivotDF(): Unit = {

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    spark.sparkContext.setLogLevel("ERROR")
    import spark.sqlContext.implicits._

    val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))

    val df = data.toDF("Product","Amount","Country")
    //df.show()

    val pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
    pivotDF.show()
  }

  def testStructure(): Unit = {

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    spark.sparkContext.setLogLevel("ERROR")


    val arrayStructureData = Seq(
      Row(Row("James ","","Smith"),List("Cricket","Movies"),Map("hair"->"black","eye"->"brown")),
      Row(Row("Michael ","Rose",""),List("Tennis"),Map("hair"->"brown","eye"->"black")),
      Row(Row("Robert ","","Williams"),List("Cooking","Football"),Map("hair"->"red","eye"->"gray")),
      Row(Row("Maria ","Anne","Jones"),null,Map("hair"->"blond","eye"->"red")),
      Row(Row("Jen","Mary","Brown"),List("Blogging"),Map("white"->"black","eye"->"black"))
    )

    val arrayStructureSchema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("hobbies", ArrayType(StringType))
      .add("properties", MapType(StringType,StringType))

    val df5 = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
    df5.printSchema()
    df5.show()

  }

  def testFilter(): Unit = {

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    spark.sparkContext.setLogLevel("ERROR")


    val arrayStructureData = Seq(
      Row(Row("James","","Smith"),List("Java","Scala","C++"),"OH","M"),
      Row(Row("Anna","Rose",""),List("Spark","Java","C++"),"NY","F"),
      Row(Row("Julia","","Williams"),List("CSharp","VB"),"OH","F"),
      Row(Row("Maria","Anne","Jones"),List("CSharp","VB"),"NY","M"),
      Row(Row("Jen","Mary","Brown"),List("CSharp","VB"),"NY","M"),
      Row(Row("Mike","Mary","Williams"),List("Python","VB"),"OH","M")
    )

    val arrayStructureSchema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("languages", ArrayType(StringType))
      .add("state", StringType)
      .add("gender", StringType)

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
    df.printSchema()
    df.filter(df("state") ===  "OH").show(false)
    df.filter("gender == 'M'").show(false)

  }

  def testJoin(): Unit = {

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    spark.sparkContext.setLogLevel("ERROR")


    val emp = Seq((1,"Smith",-1,"2018","10","M",3000),
      (2,"Rose",1,"2010","20","M",4000),
      (3,"Williams",1,"2010","10","M",1000),
      (4,"Jones",2,"2005","10","F",2000),
      (5,"Brown",2,"2010","40","",-1),
      (6,"Brown",2,"2010","50","",-1)
    )
    val empColumns = Seq("emp_id","name","superior_emp_id","year_joined",
      "emp_dept_id","gender","salary")
    import spark.sqlContext.implicits._
    val empDF = emp.toDF(empColumns:_*)
    empDF.show(false)

    val dept = Seq(("Finance",10),
      ("Marketing",20),
      ("Sales",30),
      ("IT",40)
    )

    val deptColumns = Seq("dept_name","dept_id")
    val deptDF = dept.toDF(deptColumns:_*)
    //deptDF.show(false)

    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "inner")
      .show(false)


    empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"outer")
      .show(false)
    empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"full")
      .show(false)
    empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"fullouter")
      .show(false)


    empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"leftsemi")
      .show(false)


    empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"leftanti")
      .show(false)


    empDF.createOrReplaceTempView("EMP")
    deptDF.createOrReplaceTempView("DEPT")
    //SQL JOIN
    val joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id")
    joinDF.show(false)

    val joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id")
    joinDF2.show(false)


  }

  def testDate(): Unit = {

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    spark.sparkContext.setLogLevel("ERROR")

    import spark.sqlContext.implicits._
    Seq(("04/13/2019")).toDF("Input").select(
      col("Input"),
      date_format(to_date(col("Input"), "MM/dd/yyyy"), "dd-MM-yyyy")
        .as("to_date")
    ).show()

  }

  def testAgg(): Unit = {

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    spark.sparkContext.setLogLevel("ERROR")


    import spark.implicits._

    val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )
    val df = simpleData.toDF("employee_name", "department", "salary")
    //df.show()


    //approx_count_distinct()
    println("approx_count_distinct: "+
      df.select(approx_count_distinct("salary")).collect()(0)(0))


    //avg
    println("avg: "+
      df.select(avg("salary")).collect()(0)(0))

    df.select(collect_list("salary")).show(false)

    df.select(collect_set("salary")).show(false)

    //Prints avg: 3400.0


    //Prints approx_count_distinct: 6


  }

  def testWindowFunc(): Unit = {

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    spark.sparkContext.setLogLevel("ERROR")


    import spark.implicits._

    val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )
    val df = simpleData.toDF("employee_name", "department", "salary")


    //row_number
    val windowSpec  = Window.partitionBy("department").orderBy("salary")
    df.withColumn("row_number",row_number.over(windowSpec))
      .show()


    //rank
    df.withColumn("rank",rank().over(windowSpec))
      .show()


    //dens_rank
    df.withColumn("dense_rank",dense_rank().over(windowSpec))
      .show()


    val windowSpecAgg  = Window.partitionBy("department")

    val aggDF = df.withColumn("avg", avg(col("salary")).over(windowSpecAgg))
      .withColumn("sum", sum(col("salary")).over(windowSpecAgg))
      .withColumn("min", min(col("salary")).over(windowSpecAgg))
      .withColumn("max", max(col("salary")).over(windowSpecAgg))
      .withColumn("row",row_number.over(windowSpec))
      .where(col("row")===1).select("department","avg","sum","min","max")
      .show()

    df.take(10).foreach(println)

  }
}
/**
  * Use this when submitting the app to a cluster with spark-submit
  * */
object CountingApp extends App{
  val (inputFile, outputFile) = (args(0), args(1))

  // spark-submit command should supply all necessary config elements
  Runner.run(new SparkConf(), inputFile, outputFile)
}

object Runner {
  def run(conf: SparkConf, inputFile: String, outputFile: String): Unit = {
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(inputFile)
    val counts = WordCount.withStopWordsFiltered(rdd)
    counts.saveAsTextFile(outputFile)
  }
}
