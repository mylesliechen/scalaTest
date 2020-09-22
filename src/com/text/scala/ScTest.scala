package com.text.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object ScTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("TestService")
      .master("local[*]")
      .getOrCreate()

  }
}