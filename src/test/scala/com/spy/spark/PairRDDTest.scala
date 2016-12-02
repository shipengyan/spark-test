package com.spy.spark

/**
  * Created by spy on 2016-12-01.
  */
class PairRDDTest extends BaseTest {

  test("Pair RDD Test") {
    val lines = sc.makeRDD(Seq("a 12", "b 34"), 2)
    val pairs = lines.map(x => (x.split(" ")(0), x))

    pairs.collect().foreach(println)
  }

  test("Per-key average using combineByKey() in Scala") {
    val input = sc.parallelize(List(("coffee", 1), ("coffee", 2), ("panda", 4)))
    input.values
    val result = input.combineByKey(
      (v) => (v, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
      // Note: we could us mapValues here, but we didn't because it was in the next section
    ) //.map { case (key, value) => (key, value._1 / value._2.toFloat) }

    result.collectAsMap().map(println(_))
  }
}

