package com.github.mrpowers.my.cool.project

import org.json4s.native.JsonMethods.parse
import org.json4s.DefaultFormats
import org.apache.spark.sql.SparkSession

object JsonReader extends App {
  val spark = SparkSession
    .builder
    .appName("Json reader")
    .getOrCreate()
  import spark.implicits._

  println("!!! Hello!")
  if (args.length != 1) {
    println("Path to the JSON must be the only argument")
  }
  println("!!! ", args)

  val path = args(0);
  implicit val jsonDefaultFormats: DefaultFormats = DefaultFormats
  val caseClassesCollection: Array[Wine] = spark.sparkContext.textFile(path)
    .map(input => parse(input).extract[Wine])
    .collect()

  caseClassesCollection.foreach(println)
  spark.stop()
}
