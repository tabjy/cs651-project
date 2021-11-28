package ca.uwaterloo.cs451.project

import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}
import org.bitcoinj.core.Context
import org.bitcoinj.params.MainNetParams

import scala.collection.JavaConverters.asScalaIteratorConverter

object ParseBlockSpark {
  val log = Logger.getLogger(getClass.getName)

  def main(argv: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Parse Block Spark")
    val sc = new SparkContext(conf)

    sc.binaryFiles("/home/tabjy/.bitcoin/blocks/blk02822.dat")
      .flatMap(tuple => {
        val stream = tuple._2.open()
        val np = new MainNetParams
        new Context(np) // XXX: need to construct context before parsing
        val loader = new BlockStreamLoader(np, stream)

        loader.asScala
          .flatMap(block => block.getTransactions.iterator().asScala
//            .filter(transaction => !transaction.isCoinBase)
            .map(transactions =>
              (
                transactions.getTxId.toString,
//                transactions.getInput(0).,
                transactions.getInputSum.toSat,
//                transactions.getOutput(0).getHash.toString,
                transactions.getOutputSum.toSat,
                transactions.getInputSum.toSat - transactions.getOutputSum.toSat
              )
            ))
      })
      .saveAsTextFile("output")
  }
}