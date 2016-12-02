package com.spy.spark

/**
  * Created by spy on 2016-12-01.
  */
class SimpleTest extends BaseTest {

  test("test initializing spark context") {
    val list = List(1, 2, 3, 4)
    val rdd = sc.parallelize(list)

    assert(rdd.count === list.length)
    println(rdd.count)
  }


  test("makeRDD Test") {
    val rdd = sc.makeRDD(Seq("A", "B", "C"), 2)
    rdd.zipWithIndex().collect().foreach(println)
  }


  override def beforeAll(): Unit = {
    super.beforeAll()
    println("----------before All----------");
  }

  override def afterAll(): Unit = {
    super.afterAll()
    println("----------end All----------")
  }

}
