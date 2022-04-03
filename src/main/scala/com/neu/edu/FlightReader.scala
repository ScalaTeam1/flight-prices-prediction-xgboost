package com.neu.edu

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
  val dy: Try[Dataset[Flight]] = mty map {
    mt =>
      println(s"Flight table has ${mt.size} rows")
      spark.createDataset(mt.rows.toSeq)
  }

}


object FlightReader extends App {
  def apply(resource: String): FlightReader = new FlightReader(resource)


  private val path = "/Users/arronshentu/Downloads/final/src/main/resources/test.csv"
  apply(path).dy foreach {
    d => d.show(false)
  }
}