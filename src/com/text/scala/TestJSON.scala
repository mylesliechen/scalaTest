package com.text.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object TestJSON {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TestService")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

//    val schema = StructType(
//      List(StructField("name", StringType, false),
//        StructField("age", IntegerType, false)))
//    //
//    val df = spark.read.format("minioSelectJSON").
//      schema(schema).
//      load("s3://cn-north-1-chenxue198/selectjson.json")
//
//    df.select("*").show()

    println()
    println(spark.sparkContext.hadoopConfiguration.get("fs.s3a.access.key"))
  }
}
