package com.spy.spark

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

/**
  * Created by spy on 2016-12-01.
  */
class SimpleTest extends FunSuite with SharedSparkContext {

  test("test initializing spark context") {
    val list = List(1, 2, 3, 4)
    val rdd = sc.parallelize(list)

    assert(rdd.count === list.length)
    println(rdd.count)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    println("before All");
  }

  override def afterAll(): Unit = {
    super.afterAll()
    println("end All")
  }

}
