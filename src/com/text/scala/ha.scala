package com.text.scala

import org.apache.spark.sql._
import org.apache.spark.sql.types._

class ha {
  val spark = SparkSession.builder().appName("TestService").
    master("local[*]").
    config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").
    config("spark.hadoop.fs.s3a.access.key", "").
    config("spark.hadoop.fs.s3a.secret.key", "").
    config("spark.hadoop.fs.s3a.endpoint", "").
    getOrCreate()
  val schema = StructType(List(StructField("name", StringType, false), StructField("age", IntegerType, false), StructField("work1", StringType, false)))
  val df = spark.read.format("CSV").schema(schema).options(Map("header" -> "true", "delimiter" -> ",")).load("s3a://cn-north-1-chenxue198/selecttest5.csv")

  df.select("*").filter("age > 10").show()
}
