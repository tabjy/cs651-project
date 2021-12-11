package ca.uwaterloo.cs451.project

import org.apache.log4j._
import org.apache.spark.sql.types.{FloatType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object ParseBtcPriceHistory {
  // TODO: update these
  val input = "data/bitstampUSD_1-min_data_2012-01-01_to_2021-03-31.csv"
  val output = "output/price-history"

  val log = Logger.getLogger(getClass.getName)

  def main(argv: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .appName("ParseBtcPriceHistory")
      .getOrCreate()

    val schema = StructType(
      StructField("timestamp", LongType, nullable = false) ::
        StructField("open", FloatType, nullable = false) ::
        StructField("high", FloatType, nullable = false) ::
        StructField("low", FloatType, nullable = false) ::
        StructField("close", FloatType, nullable = false) ::
        StructField("volume_btc", FloatType, nullable = false) ::
        StructField("volume_usd", FloatType, nullable = false) ::
        StructField("weighted_price", FloatType, nullable = false) :: Nil
    )

    val rdd = session.read.option("header", "true").csv(input)
      .rdd
      .map(row => Row(
        row(0).toString.toLong,
        row(1).toString.toFloat,
        row(2).toString.toFloat,
        row(3).toString.toFloat,
        row(4).toString.toFloat,
        row(5).toString.toFloat,
        row(6).toString.toFloat,
        row(7).toString.toFloat
      ))

    val df = session.sqlContext.createDataFrame(rdd, schema)
    df.show(truncate = true)
    df.write.mode(SaveMode.Overwrite).format("parquet").save(output)
  }
}