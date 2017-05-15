package com.datio.genericparser

import com.datio.genericparser.Outputs.Output
import com.datio.genericparser.Outputs.Output._
import com.datio.genericparser.Utils
import com.datio.genericparser.Utils.DataFrameUtils
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object GenericParser {


  implicit val conf = ConfigFactory.load
  val appName = conf.getString("spark.appName")
  val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster("local[*]"))
  val sQLContext = new SQLContext(sc)
  val hdfsInputPath = conf.getString("hdfs.inputPath")


  def main(args: Array[String]): Unit = {

     val utils =  Utils.Utils(sc, sQLContext)
     val dfUtils = new DataFrameUtils
     val (columns, initDf) = utils.read("/Users/germanschiavonmatteo/desarrollo/mexico/sparkgenericparser/src/main/resources/muestra1872.csv")


    val result = dfUtils.changeDataFrameSchema(initDf)
    Output(result).saveAvroModeToHDFS()

  }

}

