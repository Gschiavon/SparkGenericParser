import akka.actor.Status

import scala.util.{Failure, Success, Try}

object Testing {

  def main(args: Array[String]): Unit = {


    val p = "   .select(\"CD_TARJETA\", \"NU_BIN_TARJETA\", \"CD_GIRO\",\"IX_CROSS_BORDER\", \"TP_POS_ENTRY_MODE1\", \"TP_POS_ENTRY_MODE2\"\n           ,\"TP_LIBRE_5\", \"CD_RETORNO\", \"CD_RAZON_RET\",\"CD_OPERACION\", \"TP_MENSAJE\", \"IM_TRANSACCION\",\"CD_CANAL\", \"FH_OPERACION\", \"TP_ECI\")\n       .withColumn(\"CD_TARJETA\", stringToLong(dataFrame(\"CD_TARJETA\")))\n       .withColumn(\"NU_BIN_TARJETA\", stringToInt(dataFrame(\"NU_BIN_TARJETA\")))\n       .withColumn(\"CD_GIRO\", stringToInt(dataFrame(\"CD_GIRO\")))\n       .withColumn(\"IX_CROSS_BORDER\", stringToInt(dataFrame(\"IX_CROSS_BORDER\")))\n       .withColumn(\"TP_POS_ENTRY_MODE1\", stringToInt(dataFrame(\"TP_POS_ENTRY_MODE1\")))\n       .withColumn(\"TP_POS_ENTRY_MODE2\", stringToInt(dataFrame(\"TP_POS_ENTRY_MODE2\")))\n       .withColumn(\"TP_LIBRE_5\", stringToInt(dataFrame(\"TP_LIBRE_5\")))\n       .withColumn(\"CD_RETORNO\", stringToInt(dataFrame(\"CD_RETORNO\")))\n       .withColumn(\"CD_RAZON_RET\", stringToInt(dataFrame(\"CD_RAZON_RET\")))\n       .withColumn(\"CD_OPERACION\", stringToInt(dataFrame(\"CD_OPERACION\")))\n       .withColumn(\"TP_MENSAJE\", stringToInt(dataFrame(\"TP_MENSAJE\")))"


    println(p.toLowerCase)
  }

}
