package com.spy.spark

import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by spy on 2016-11-20.
  */
object App {

  def main(args: Array[String]): Unit = {
    println("app");
    val sparkConf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(sparkConf)

    //    val rdd = sc.parallelize(List("a", "b"))
    val rdd = sc.parallelize(1 to 100, 4)
    println(rdd.count())

    println(rdd.collect().foreach(println))

    println("方法二-----------------")

    println(rdd.take(50).foreach(println))

    println("方法三------");
    println(s"${rdd.collect().mkString(", ")}")

  }
}
