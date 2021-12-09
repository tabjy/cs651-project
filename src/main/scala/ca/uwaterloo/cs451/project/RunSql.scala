package ca.uwaterloo.cs451.project

import org.apache.spark.sql.SparkSession

object RunSql {
  def main(argv: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .appName("ParseBlockSpark")
      .getOrCreate()

    session.read.parquet("/home/tabjy/.bitcoin/dataset/blocks").createOrReplaceTempView("blocks")
    session.read.parquet("/home/tabjy/.bitcoin/dataset/transactions").createOrReplaceTempView("transactions")
    session.read.parquet("/home/tabjy/.bitcoin/dataset/inputs").createOrReplaceTempView("inputs")
    session.read.parquet("/home/tabjy/.bitcoin/dataset/outputs").createOrReplaceTempView("outputs")

    session.sql(argv(0)).show(truncate = false)
  }
}
