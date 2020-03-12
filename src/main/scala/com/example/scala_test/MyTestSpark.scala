package com.example.scala_test

import org.apache.spark.sql.{DataFrame, SparkSession}

object MyTestSpark {
  def printTest :Unit = println("hello")

  def readCSVToDataFarme(spark: SparkSession): DataFrame = {
    val myDF = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load("files/file.csv")
    return myDF
  }
}
