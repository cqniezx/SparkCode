package com.zhixue

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


object Test0503 {

  def getTest(str: String): String = {
    str.split("_")(1)
  }

  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
//    val sc = new SparkContext(conf)
//    // 这个在官网基础上添加，使得输出级别为WARN级别，避免INFO信息过多。
//    // 多说一句，其实Python对Spark日志的几乎相同。
//    sc.setLogLevel("WARN")
//    // 为了便于看清楚对流数据的Word count，把时间间隔设置为5s。
//    val ssc = new StreamingContext(sc, Seconds(5))
//
//    // Create a DStream that will connect to hostname:port, like localhost:9999
//    val lines = ssc.socketTextStream("localhost", 9999)
//    // Split each line into words
//    val words = lines.flatMap(_.split(" "))
//    // Count each word in each batch
//    val pairs = words.map(word => (word, 1))
//    val wordCounts = pairs.reduceByKey(_ + _)
//
//    // Print the first ten elements of each RDD generated in this DStream to the console
//    wordCounts.print()
//    ssc.start()             // Start the computation
//    ssc.awaitTermination()

    val spark = SparkSession.builder().appName("Test0503")
      .master("local[2]")
      .getOrCreate()
    val dataList = List((1,2,3),(2,3,4),(4,5,6))
    val colsName = List("col1", "col2", "col3")
    val df = spark.createDataFrame(dataList).toDF(colsName:_*)
    df.show()


  }

}
