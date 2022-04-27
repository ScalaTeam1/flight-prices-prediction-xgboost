package com.neu.edu.FlightPricePrediction.pojo

import com.phasmidsoftware.table.Table
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Try

case class FlightReader(resource: String) {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("xiangdangdang")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  private val mty: Try[Table[Flight]] = Table.parseFile[Table[Flight]](resource)
  val dy: Try[Dataset[Flight]] = mty map { mt =>
    println(s"Flight table has ${mt.size} rows")
    spark.createDataset(mt.rows.toSeq)
  }
}

case class IterableFlightReader(resource: Seq[Flight]) {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("xiangdangdang")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val dy: Dataset[Flight] = spark.createDataset(resource)
}

object FlightReader extends App {
  def apply(resource: String): FlightReader = new FlightReader(resource)
}
