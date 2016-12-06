package com.spy.spark.loadAndSaveData

import java.io.{StringReader, StringWriter}

import au.com.bytecode.opencsv.{CSVReader, CSVWriter}
import com.spy.spark.BaseTest
import com.spy.spark.utils.FileUtil

import scala.collection.JavaConversions._


/**
  * Created by spy on 2016-12-05.
  */

class CSVTest extends BaseTest {
  val inputFile = "d:/test/spark/person.csv"
  val outputFile = "d:/test/spark/output"


  override def beforeAll(): Unit = {
    super.beforeAll()
    FileUtil.deleteFolder(outputFile)
  }

  test("Load CSV Test") {
    case class Person(name: String, favouriteAnimal: String)

    val input = sc.textFile(inputFile)
    val result = input.map { line =>
      val reader = new CSVReader(new StringReader(line));
      reader.readNext();
    }

    val people = result.map(x => Person(x(0), x(1)))
    val pandaLovers = people.filter(person => person.favouriteAnimal == "panda")
    pandaLovers.map(person => List(person.name, person.favouriteAnimal).toArray).mapPartitions { people =>
      val stringWriter = new StringWriter();
      val csvWriter = new CSVWriter(stringWriter)
      csvWriter.writeAll(people.toList)
      Iterator(stringWriter.toString)
    }.saveAsTextFile(outputFile)
  }


}
