package com.datio.genericparser.Outputs

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode._


case class Output(dataFrame: DataFrame) {

  def saveParquetModeToHDFS(partitionColumn: Option[String] = None)(implicit conf: Config) = {
    val hdfsOutputPath = conf.getString("hdfs.outputPath")
    partitionColumn match {
      case Some(col) =>  dataFrame.write.mode(Append).partitionBy(col).parquet(hdfsOutputPath)
      case None => dataFrame.write.mode(Append).parquet(hdfsOutputPath)
    }
  }
}
