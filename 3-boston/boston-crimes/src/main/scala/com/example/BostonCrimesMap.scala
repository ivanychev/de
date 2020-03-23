package com.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

case class InputData(rawCrimes: DataFrame, rawOffenceCodes: DataFrame)

object BostonCrimesMap extends SparkSessionWrapper {

  import spark.implicits._

  val TopCrimeTypeCount = 3

  def main(args: Array[String]): Unit = {
    val crimeCsvPath = args(0)
    val offenceCodesCsvPath = args(1)
    val outputFolderPath = args(2)

    val data = readData(crimeCsvPath, offenceCodesCsvPath)

    val percentileFunc = expr("percentile_approx(crimes_monthly, 0.5)");

    // Finding out monthly median of crime rates per district.
    val monthlyCrimes = data.rawCrimes
      // Calculating number of crimes per each district and year-month pair
      .groupBy($"DISTRICT", $"YEAR", $"MONTH")
      .agg(count("*").as("crimes_monthly"))
      // Then, find a median of calculated rates per district.
      .groupBy($"DISTRICT")
      .agg(percentileFunc.alias("crimes_monthly"))

    // Preparing a window for rank() that finds top crime types in each district.
    val crimeTypeCountColumn = "crime_type_count"
    val perDistrictWindow = Window
      .partitionBy($"DISTRICT")
      .orderBy(desc(crimeTypeCountColumn));

    val topCrimeTypes = data.rawCrimes
      // Counting each type of crime in each district.
      .groupBy($"DISTRICT", $"OFFENSE_CODE")
      .agg(count("OFFENSE_CODE").as(crimeTypeCountColumn))
      // Ranking them in the descending order and taking top N.
      .withColumn("offence_rank", rank().over(perDistrictWindow))
      .where($"offence_rank" <= TopCrimeTypeCount)
      .orderBy($"DISTRICT", $"offence_rank")
      // Translating crime codes to crime names.
      .join(broadcast(data.rawOffenceCodes), $"OFFENSE_CODE" === data.rawOffenceCodes("CODE"))
      // ...and collapsing the crime names to lists.
      .groupBy($"DISTRICT")
      .agg(collect_set("pretty_name").alias("frequent_crime_types"))

    val allAggregates = data.rawCrimes
      .groupBy($"DISTRICT")
      .agg(
        count("*").alias("crimes_total"),
        avg("Lat").alias("lat"),
        avg("Long").alias("lng"))
      .join(topCrimeTypes, "DISTRICT")
      .join(monthlyCrimes, "DISTRICT")
      .select($"DISTRICT", $"crimes_total", $"crimes_monthly", $"frequent_crime_types", $"lat", $"lng")

    allAggregates.repartition(1).write
      .format("parquet")
      .mode("append")
      .save(outputFolderPath)

    println("All done.")
  }

  def readData(crimesPath: String, offencesPath: String): InputData = {
    val rawCrimes: DataFrame = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(crimesPath)
      .cache()

    val rawOffenceCodes = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(offencesPath)
      .withColumn("pretty_name", split($"NAME", "\\s*-\\s*")(0))

    InputData(rawCrimes, rawOffenceCodes)
  }
}
