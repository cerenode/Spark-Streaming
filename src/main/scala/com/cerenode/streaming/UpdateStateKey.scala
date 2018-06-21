package com.cerenode.streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.log4j.{Level, Logger}
object StatefulNetworkWordCount {

  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val newCount = runningCount.getOrElse(0) + newValues.sum
    Some(newCount)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("StatefulNetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(2))

    // Set checkpoint directory
    ssc.checkpoint(".")

    Logger.getRootLogger.setLevel(Level.ERROR)

    val lines = ssc.socketTextStream("localhost", 9999)


    val words = lines.flatMap(_.split(" "))


    val pairs = words.map(word => (word, 1))


    val runningCounts = pairs.updateStateByKey[Int](updateFunction _)


    runningCounts.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}