package com.spy.spark

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

/**
  * Created by spy on 2016-12-01.
  */
class BaseTest extends FunSuite with SharedSparkContext {

  override def beforeAll(): Unit = {
    super.beforeAll() //TODO 注意下这里
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }
}
