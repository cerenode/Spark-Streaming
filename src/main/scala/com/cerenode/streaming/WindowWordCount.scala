package com.cerenode.streaming

package com.cerenode.streaming
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}


object WindowkWordCount {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(4))

    Logger.getRootLogger.setLevel(Level.ERROR)
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))


    //val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    //wordCounts.print()

    val windowedWordCounts = pairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(10), Seconds(8))

    windowedWordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

