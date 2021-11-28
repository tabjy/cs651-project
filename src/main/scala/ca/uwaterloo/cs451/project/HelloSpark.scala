package ca.uwaterloo.cs451.project

import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}

object HelloSpark {
  val log = Logger.getLogger(getClass.getName)

  def main(argv: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Hello Spark")
    val sc = new SparkContext(conf)

    log.info("Hell spark!")
  }
}