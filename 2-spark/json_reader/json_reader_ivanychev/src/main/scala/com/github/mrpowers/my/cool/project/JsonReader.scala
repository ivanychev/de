package com.github.mrpowers.my.cool.project
import org.json4s.native.JsonMethods.parse
import org.json4s.DefaultFormats


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object JsonReader extends SparkSessionWrapper {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Path to the JSON must be the only argument")
      return
    }

    val path = args(1);
    implicit val jsonDefaultFormats: DefaultFormats = DefaultFormats
    val caseClassesCollection: Array[Wine] = spark.sparkContext.textFile(path)
      .map(input => parse(input).extract[Wine])
      .collect()

    caseClassesCollection.foreach(println)
  }
}
