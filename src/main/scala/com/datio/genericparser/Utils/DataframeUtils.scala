package com.datio.genericparser.Utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.udf


 class DataframeUtils {

  val stringToInt = udf { (value: String) =>

    if(value != null && !value.equals("?"))
      value.toString
    else
      "0"
  }

   val poutcomeToInt = udf { (value: String) =>
     value match {
       case "unknown" => 1
       case "failure" => 2
       case "other" => 3
       case _ => 0

     }
   }



}
