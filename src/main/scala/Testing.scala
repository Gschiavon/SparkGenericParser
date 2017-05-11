import akka.actor.Status

import scala.util.{Failure, Success, Try}

object Testing {

  def main(args: Array[String]): Unit = {


    val numberList = List(1,2,3)
    val charList = List("Uno", "Dos", "Tres")

    var pairs = (for ((m, a) <- (numberList zip charList)) yield (m, a)).toMap


    println(pairs.get(2).get)

  }

}
