package com.spy.spark

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by spy on 2016-12-01.
  */
object PersistTest {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(sparkConf)

    //    val input = List("abc", "sss", "xxx")
    val input = sc.parallelize(1 to 100, 4)


    val result = input.map(x => x * x)
    result.persist(StorageLevel.DISK_ONLY)
    println(result.count())
    println(result.collect().mkString(",\r\n"))
  }
}
