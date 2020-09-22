package com.text.scala

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.{EqualTo, Filter, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual}
import org.apache.spark.sql.types.{DataType, DateType, IntegerType, StringType, StructField, StructType, TimestampType}

object TestSource {
  def main(args: Array[String]): Unit = {
    val schema = StructType(
      List(StructField("name", StringType, false),
        StructField("age", IntegerType, false),
        StructField("work1", StringType, false)))

    val str = queryFromSchema(schema, null)
    println(str)
  }

  def buildWhereClause(schema: StructType, filters: Seq[Filter]): String = {
    val filterExpressions = filters.flatMap(f => buildFilterExpression(schema, f)).mkString(" AND ")
    if (filterExpressions.isEmpty) "" else "WHERE " + filterExpressions
  }


  def buildFilterExpression(schema: StructType, filter: Filter): Option[String] = {
    def buildComparison(attr: String, value: Any, comparisonOp: String): Option[String] = {
      getTypeForAttribute(schema, attr).map { dataType =>
        val sqlEscapedValue: String = dataType match {
          case StringType => s""""${value.toString.replace("'", "\\'\\'")}""""
          case DateType => s""""${value.asInstanceOf[Date]}""""
          case TimestampType => s""""${value.asInstanceOf[Timestamp]}""""
          case _ => value.toString
        }
        s"s." + s""""$attr"""" + s" $comparisonOp $sqlEscapedValue"
      }
    }

    filter match {
      case EqualTo(attr, value) => buildComparison(attr, value, "=")
      case LessThan(attr, value) => buildComparison(attr, value, "<")
      case GreaterThan(attr, value) => buildComparison(attr, value, ">")
      case LessThanOrEqual(attr, value) => buildComparison(attr, value, "<=")
      case GreaterThanOrEqual(attr, value) => buildComparison(attr, value, ">=")
      case _ => None
    }
  }

  private def getTypeForAttribute(schema: StructType, attribute: String): Option[DataType] = {
    if (schema.fieldNames.contains(attribute)) {
      Some(schema(attribute).dataType)
    } else {
      None
    }
  }

  def queryFromSchema(schema: StructType, filters: Array[Filter]): String = {
    var columnList = schema.fields.map(x => s"s." + s""""${x.name}"""").mkString(",")
    if (columnList.length == 0) {
      columnList = "*"
    }
    println(columnList)
//    columnList = columnList.replace("\"", "")
    val whereClause = buildWhereClause(schema, filters)
    if (whereClause.length == 0) {
      s"select $columnList from S3Object s"
    } else {
      s"select $columnList from S3Object s $whereClause"
    }
  }


}
