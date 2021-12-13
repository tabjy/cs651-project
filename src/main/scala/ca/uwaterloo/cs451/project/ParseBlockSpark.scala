package ca.uwaterloo.cs451.project

import org.apache.hadoop.fs.Path
import org.apache.log4j._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.bitcoinj.core.Context
import org.bitcoinj.params.MainNetParams
import org.bitcoinj.script.ScriptException

import scala.collection.JavaConverters.asScalaIteratorConverter

object ParseBlockSpark {
  // TODO: update these
  val input = "/home/tabjy/.bitcoin/blocks"
  val output = "/home/tabjy/.bitcoin/dataset"

  val log = Logger.getLogger(getClass.getName)

  def main(argv: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .appName("ParseBlockSpark")
      .getOrCreate()

    val blockSchema = StructType(
      StructField("hash", StringType, nullable = false) ::
        StructField("time", LongType, nullable = false) ::
        StructField("file", StringType, nullable = false) ::
        StructField("_partition", StringType, nullable = false) :: Nil
    )

    val transactionSchema = StructType(
      StructField("hash", StringType, nullable = false) ::
        StructField("block", StringType, nullable = false) ::
        StructField("is_coinbase", BooleanType, nullable = false) ::
        StructField("lock_time", LongType, nullable = false) ::
        StructField("_partition", StringType, nullable = false) :: Nil
    )

    val inputSchema = StructType(
      StructField("transaction", StringType, nullable = false) ::
        StructField("index", IntegerType, nullable = false) ::
        StructField("parent_transaction", StringType, nullable = false) ::
        StructField("vout", IntegerType, nullable = false) ::
        StructField("_partition", StringType, nullable = false) :: Nil
    )

    val outputSchema = StructType(
      StructField("transaction", StringType, nullable = false) ::
        StructField("index", IntegerType, nullable = false) ::
        StructField("value", LongType, nullable = false) ::
        StructField("address", StringType, nullable = true) ::
        StructField("_partition", StringType, nullable = false) :: Nil
    )

    val blocks = session.sparkContext.binaryFiles(input + "/blk015**.dat")
      .flatMap(tuple => {
        val stream = tuple._2.open()
        val np = new MainNetParams
        new Context(np) // XXX: need to construct context before parsing
        val loader = new BlockStreamLoader(np, stream)

        val file = new Path(tuple._2.getPath()).getName
        loader.asScala.map(block => (block, file))
      })

    val blockRdd = blocks.map(tuple => {
      Row(tuple._1.getHashAsString, tuple._1.getTimeSeconds, tuple._2, tuple._2.substring(3, 8))
    })

    val blockDf = session.sqlContext.createDataFrame(blockRdd, blockSchema)
    blockDf.show(truncate = true)
    blockDf.write.mode(SaveMode.Overwrite).partitionBy("_partition").format("parquet").save(output + "/blocks")

    val transactionRdd = blocks
      .flatMap(tuple => {
        val block = tuple._1
        val partition = tuple._2.substring(3, 8)
        block.getTransactions.iterator().asScala.map(transaction => {
          Row(transaction.getTxId.toString, block.getHashAsString, transaction.isCoinBase, transaction.getLockTime, partition)
        })
      })

    val transactionDf = session.sqlContext.createDataFrame(transactionRdd, transactionSchema)
    transactionDf.show(truncate = true)
    transactionDf.write.mode(SaveMode.Overwrite).partitionBy("_partition").format("parquet").save(output + "/transactions")

    val inputRdd = blocks
      .flatMap(tuple => {
        val partition = tuple._2.substring(3, 8)
        tuple._1.getTransactions.iterator().asScala.flatMap(transaction => {
          val txId = transaction.getTxId.toString
          transaction.getInputs.iterator().asScala.map(input => {
            Row(txId, input.getIndex, input.getParentTransaction.getTxId.toString, input.getOutpoint.getIndex.toInt, partition)
          })
        })
      })

    val inputDf = session.sqlContext.createDataFrame(inputRdd, inputSchema)
    inputDf.show(truncate = true)
    inputDf.write.mode(SaveMode.Overwrite).partitionBy("_partition").format("parquet").save(output + "/inputs")

    val outputRdd = blocks
      .flatMap(tuple => {
        val np = new MainNetParams
        val partition = tuple._2.substring(3, 8)
        tuple._1.getTransactions.iterator().asScala.flatMap(transaction => {
          val txId = transaction.getTxId.toString
          transaction.getOutputs.iterator().asScala.map(output => {
            var address : String = null
            try {
              address = if (output.getAddressFromP2SH(np) != null) {
                output.getAddressFromP2SH(np).toString
              } else if (output.getAddressFromP2PKHScript(np) != null) {
                output.getAddressFromP2PKHScript(np).toString
              } else {
                null
              }
            } catch {
              case e: ScriptException => if (!transaction.isCoinBase) log.warn("Incorrectly structured script", e)
            }

            Row(txId, output.getIndex, output.getValue.toSat, address, partition)
          })
        })
      })

    val outputDf = session.sqlContext.createDataFrame(outputRdd, outputSchema)
    outputDf.show(truncate = true)
    outputDf.write.mode(SaveMode.Overwrite).partitionBy("_partition").format("parquet").save(output + "/outputs")
  }
}