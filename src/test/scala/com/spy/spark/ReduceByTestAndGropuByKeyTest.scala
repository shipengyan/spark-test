package com.spy.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by spy on 2016-12-01.
  */
object ReduceByTestAndGropuByKeyTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(sparkConf)

    val words = Array("one", "two", "two", "three", "three", "three")

    val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))

    val wordCountsWithReduce = wordPairsRDD.reduceByKey(_ + _)
    println(wordCountsWithReduce)

    val wordCountsWithGroup = wordPairsRDD.groupByKey().map(t => (t._1, t._2.sum))
    println(wordCountsWithGroup)

    //    sc.makeRDD()

  }
}
