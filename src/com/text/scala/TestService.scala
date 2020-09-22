package com.text.scala

import org.apache.spark.sql._
import org.apache.spark.sql.types._

object TestService {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("TestService").master("local[*]").config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").config("spark.hadoop.fs.s3a.access.key", "A4C4326339D0787E96C40510576A8C69").config("spark.hadoop.fs.s3a.secret.key", "2811B0CCD927736BC62BE14B7E2FD4E5").config("spark.hadoop.fs.s3a.endpoint", "http://s3.cn-north-1.jdcloud-oss.com").getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    val schema = StructType(
      List(StructField("name", StringType, false),
        StructField("age", IntegerType, false)))

    val df = spark.read.format("minioSelectCSV").
      schema(schema).
      options(Map("quote" -> "\'", "header" -> "true", "delimiter" -> ",")).
      load("s3://cn-north-1-chenxue198/seletctestdir/")
    df.show()

    //    spark.sql("CREATE TEMPORARY VIEW MyView (age INT, name STRING) USING minioSelectCSV OPTIONS (path \"s3://cn-north-1-chenxue198/selecttest5.csv\")")
    //    spark.sql("select * from MyView where age > 10").show()
    //org/apache/hadoop/fs/staging/StagingDirectoryCapable
  }
}

