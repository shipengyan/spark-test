package com.spy.spark.lang

import com.spy.spark.BaseTest

/**
  * Created by spy on 2016-12-05.
  */
class MapTest extends BaseTest {

  test("mapPartitions Test") {
    def sumOfEveryPartition(input: Iterator[Int]): Int = {
      println("has one partition")

      var total = 0
      input.foreach { elem =>
        total += elem
      }
      total
    }

    val input = sc.parallelize(List(1, 2, 3, 4, 5, 6), 2) //RDD有6个元素，分成2个partition
    val result = input.mapPartitions(
      partition => Iterator(sumOfEveryPartition(partition)))
    result.collect().foreach(println)
    //partition是传入的参数，是个list，要求返回也是list，即Iterator(sumOfEveryPartition(partition))
  }


}
