package com.text.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object AwsS3SElect {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("AwsS3SElect")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.region", "us-west-2")

    val schema = StructType(
      List(StructField("name", StringType, false),
        StructField("age", StringType, false),
        StructField("address1", StringType, false),
        StructField("work1", StringType, false)))

    val df = spark.read.format("minioSelectCSV").
      schema(schema).
      options(Map("quote" -> "\'", "header" -> "true", "delimiter" -> ",")).
      load("s3://")

    df.select("*").show()
  }
}
