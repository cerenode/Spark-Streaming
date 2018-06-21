package com.cerenode.streaming
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}


object NetworkWordCount {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[*]")
   // val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sparkConf, Seconds(1))

    Logger.getRootLogger.setLevel(Level.ERROR)

    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))

    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
