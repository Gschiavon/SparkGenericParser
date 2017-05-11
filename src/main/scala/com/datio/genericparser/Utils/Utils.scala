package com.datio.genericparser.Utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.util.{Failure, Success, Try}


case class Utils(sparkContext: SparkContext, sQLContext: SQLContext) {


  def buildSchema(columnNames: List[String], structFields: List[DataType]): StructType = {


    val pairs = (for ((m, a) <- (columnNames zip structFields)) yield (m, a)).toMap

    //    StructType(columnNames.map(colName => {
    //      StructField(colName, pairs.get(colName).get, false)
    //    }))

    StructType(columnNames.map(colName => StructField(colName, StringType, false)))
  }


  def getStructFields(rdd: RDD[String]): List[DataType] = {
    val sample = rdd
      .take(2)
      .drop(1)
      .map(_.split(",").toList)

    inferDataTypes(sample)
  }

  def read(resource: String): (List[String], DataFrame) = {
    val rdd = sparkContext.textFile(resource)

    val header = rdd.first.split(",").to[List]

    val structFields = getStructFields(rdd)
    val schema = buildSchema(header, structFields)


    val data =
      rdd
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it)
        .map(_.split(",").toList)
        .map(lines => Row(lines.map(line => line): _*))

    val dataFrame =
      sQLContext.createDataFrame(data, schema)

    (header, dataFrame)
  }

  /**
    *
    * @param data List of values of the CSV
    * @return a List with type values for DataFrames (StringType, IntegerType, DoubleType)
    */

  def inferDataTypes(data: Array[List[String]]): List[DataType] = {

    for {
      value <- data(0)
    } yield stringToStructField(value)

  }


  /**
    *
    * @param field from the CSV
    * @return DataFrame Structfield type
    */
  def stringToStructField(field: String): DataType = {

    if(field.contains(".")){
      if(field.contains(",")) {
        Try(field.replace(",","").toDouble)
        match {
          case Success(value) => DoubleType
          case Failure(_) => StringType
        }
      } else {
        Try(field.toDouble)
        match {
          case Success(value) => DoubleType
          case Failure(_) => StringType
        }
      }
    } else {
      Try(field.toInt)
      match {
        case Success(value) => IntegerType
        case Failure(_) => StringType
      }
    }
  }

}
