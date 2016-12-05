package com.spy.spark.loadAndSaveData

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.spy.spark.BaseTest
import com.spy.spark.utils.FileUtil

case class Person(name: String, lovesPandas: Boolean) {
  def this() {
    this("no name", false)
  }

  //  def this(name: String, lovesPandas: Boolean) {
  //    this(name, lovesPandas)
  //  }
}

// Note: must be a top level class

/**
  * Created by spy on 2016-12-05.
  */
class JsonTest extends BaseTest {
  val inputFile = "d:/test/spark/person.json"
  val outputFile = "d:/test/spark/output"

  override def beforeAll(): Unit = {
    super.beforeAll()
    FileUtil.deleteFolder(outputFile)
  }

  test("Basic Load JSON") {
    val input = sc.textFile(inputFile)

    // Parse it into a specific case class. We use mapPartitions beacuse:
    // (a) ObjectMapper is not serializable so we either create a singleton object encapsulating ObjectMapper
    //     on the driver and have to send data back to the driver to go through the singleton object.
    //     Alternatively we can let each node create its own ObjectMapper but that's expensive in a map
    // (b) To solve for creating an ObjectMapper on each node without being too expensive we create one per
    //     partition with mapPartitions. Solves serialization and object creation performance hit.
    val result = input.mapPartitions(records => {


      // mapper object created on each executor node
      val mapper = new ObjectMapper with ScalaObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)
      // We use flatMap to handle errors
      // by returning an empty list (None) if we encounter an issue and a
      // list with one element if everything is ok (Some(_)).


      records.flatMap(record => {
        try {
          Some(mapper.readValue(record, classOf[Person]))
        } catch {
          case e: Exception => None
        }
      })
    }, true)

    //    println("--------")
    //    result.collect().foreach(println)

    result.filter(_.lovesPandas).mapPartitions(records => {
      //      println(">>>>>")

      val mapper = new ObjectMapper with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)

      //      println("/////")
      //      records.foreach(println)
      // println输出会影响saveAsTextFile内容

      records.map(mapper.writeValueAsString(_))

    })
      .saveAsTextFile(outputFile)


  }

  test("Basic Load JSON by fastjson") {
    val input = sc.textFile(inputFile)

    val result = input.mapPartitions(records => {
      records.flatMap(record => {
        //try {
        println(record)
        Some(JSON.parseObject(record, classOf[Person]))
        //} catch {
        //  case e: Exception => None
        //}
      })
    }, true)

    //    result.collect().foreach(println)

    result.filter(_.lovesPandas).mapPartitions(records => {
      //TODO 有点问题哈
      records.map(JSON.toJSONString(_, SerializerFeature.PrettyFormat))
      //      records.map(record => Math.random())
    })
      .saveAsTextFile(outputFile)
  }

}
