package com.datio.genericparser.Utils



import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.udf


class DataFrameUtils {

  val stringToInt = udf { (value: Any) =>
    if(value != null && !value.equals("?"))
      value.toString.toInt
    else
      0
  }

  val stringToLong = udf {(value: Any) =>
    if(value != null && !value.equals("?"))
      value.toString.toLong
    else
      0L
  }

  def changeDataFrameSchema(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select("cd_tarjeta", "nu_bin_tarjeta", "cd_giro","ix_cross_border", "tp_pos_entry_mode1", "tp_pos_entry_mode2"
        ,"tp_libre_5", "cd_retorno", "cd_razon_ret","cd_operacion", "tp_mensaje", "im_transaccion","cd_canal"
        ,"fh_operacion", "tp_eci", "cd_adquirente", "nu_afiliacion", "tp_cuenta", "tp_cuenta_destino")
      .withColumn("cd_tarjeta", stringToLong(dataFrame("cd_tarjeta")))
      .withColumn("nu_bin_tarjeta", stringToInt(dataFrame("nu_bin_tarjeta")))
      .withColumn("cd_giro", stringToInt(dataFrame("cd_giro")))
      .withColumn("ix_cross_border", stringToInt(dataFrame("ix_cross_border")))
      .withColumn("tp_pos_entry_mode1", stringToInt(dataFrame("tp_pos_entry_mode1")))
      .withColumn("tp_pos_entry_mode2", stringToInt(dataFrame("tp_pos_entry_mode2")))
      .withColumn("tp_libre_5", stringToInt(dataFrame("tp_libre_5")))
      .withColumn("cd_retorno", stringToInt(dataFrame("cd_retorno")))
      .withColumn("cd_razon_ret", stringToInt(dataFrame("cd_razon_ret")))
      .withColumn("cd_operacion", stringToInt(dataFrame("cd_operacion")))
      .withColumn("tp_mensaje", stringToInt(dataFrame("tp_mensaje")))
      .withColumn("nu_afiliacion", stringToInt(dataFrame("nu_afiliacion")))
      .withColumn("tp_cuenta", stringToInt(dataFrame("tp_cuenta")))
      .withColumn("tp_cuenta_destino", stringToInt(dataFrame("tp_cuenta_destino")))

  }

  def changeCdGiro(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select("cd_giro")
      .withColumn("cd_giro", stringToInt(dataFrame("cd_giro")))
  }
}


