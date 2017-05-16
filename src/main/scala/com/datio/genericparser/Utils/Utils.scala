package com.datio.genericparser.Utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}


case class Utils(sparkContext: SparkContext, sQLContext: SQLContext) extends Serializable {


  def buildSchema(columnNames: List[String]): StructType = {
    StructType(columnNames.map(colName => StructField(colName, StringType, true)))
  }


  def read(resource: String): (List[String], DataFrame) = {
    val rdd = sparkContext.textFile(resource)

    val header = rdd.first.split("\\|").map(_.toLowerCase).to[List]
    val schema = buildSchema(header)
    val data =
      rdd
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it)
        .map(line => line.split("\\|", -1).toList)
        .map(lines => Row(lines.map(line => line): _*))

    val dataFrame =
      sQLContext.createDataFrame(data, schema)

    (header, dataFrame)
  }

  def parseSaldo(saldo: String): String = {
    val regex = "[0-9],[0-9][0-9][0-9].[0-9][0-9]"
    if(saldo.matches(regex))
      saldo.replace(",","")
    else saldo
  }

}
