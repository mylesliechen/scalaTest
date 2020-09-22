package com.text.scala;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SelectTest {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .master("local[*]")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.access.key", "")
                .config("spark.hadoop.fs.s3a.secret.key", "")
                .config("spark.hadoop.fs.s3a.endpoint", "")
                .config("fs.s3a.connection.ssl.enabled", "false").config("spark.network.timeout", "600s")
                .config("spark.sql.codegen.wholeStage", "false").config("spark.executor.heartbeatInterval", "500s")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("mapreduce.fileoutputcommitter.algorithm.version", "2")
                .config("fs.s3a.connection.establish.timeout", "501000").config("fs.s3a.connection.timeout", "501000")
                .getOrCreate();


        Dataset<Row> s3selectCSV = session.read().format("CSV").load("s3a://");
        s3selectCSV.select("*").show();


    }

}
