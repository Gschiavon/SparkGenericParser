package Utils

import com.datio.genericparser.Utils.Utils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Matchers, WordSpec}


class UtilsSuite extends WordSpec with Matchers {

//  val sc = new SparkContext(new SparkConf().setAppName("Test").setMaster("local[*]"))
//  val sQLContext = new SQLContext(sc)
//  val utils = new Utils(sc, sQLContext)

//  "checkDataTypes" should {
//
//    "Testing checkDataTypes with a String" in {
//      val data = "hola"
//      val result = utils.checkDataTypes(data)
//
//      result should be ("StringType")
//    }
//
//    "Testing checkDataTypes with an Int" in {
//      val data = 25
//      val result = utils.checkDataTypes(data)
//
//      result should be ("IntegerType")
//    }
//
//    "Testing checkDataTypes with a Double" in {
//      val data = 25.99
//      val result = utils.checkDataTypes(data)
//
//      result should be ("DoubleType")
//    }
//  }

 /* "stringToStructField" should {

    "Testing it with a Double " in {
      val data = "25.99"
      val result = utils.stringToStructField(data)

      result should be (DoubleType)
    }

    "Testing it with a Double with comma" in {
      val data = "2,555.99"
      val result = utils.stringToStructField(data)

      result should be (DoubleType)
    }

    "Testing it with an Int" in {
      val data = "5422"
      val result = utils.stringToStructField(data)

      result should be (IntegerType)
    }

    "Testing it with a String" in {
      val data = "BBVA"
      val result = utils.stringToStructField(data)

      result should be (StringType)
    }

  }

   "inferDataTypes should return a List[String] with the DataTypes for the DataFrame" should {

     "Testing inferDataTypes of a List(String, String, Int) should return " +
       "List(StringType, StringType, IntegerType)" in {
       val data = Array(List("value1", "5,8457.36", "22"))
       val result = utils.inferDataTypes(data)

       result should be(List(StringType, DoubleType, IntegerType))
     }

   }

*/


}
