package com.cerenode.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.log4j.{Level, Logger}


object SqlNetworkWordCount {
  def main(args: Array[String]) {


    val sparkConf = new SparkConf().setAppName("SqlNetworkWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(4))
    Logger.getRootLogger.setLevel(Level.ERROR)


    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))


    words.foreachRDD { (rdd: RDD[String], time: Time) =>
      val spark =   SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._


      // Convert RDD[String] to RDD[case class] to DataFrame
      val wordsDataFrame = rdd.map(w => Record(w)).toDF()

      // Creates a temporary view using the DataFrame
      wordsDataFrame.createOrReplaceTempView("words")

      // Do word count on table using SQL and print it
      val wordCountsDataFrame =
        spark.sql("select word, count(*) as total from words group by word")
      println(s"========= $time =========")
      wordCountsDataFrame.show()
    }

    ssc.start()
    ssc.awaitTermination()
  }
}


/** Case class for converting RDD to DataFrame */
case class Record(word: String)

