package com.neu.edu.FlightPricePrediction.db

import com.neu.edu.FlightPricePrediction.db.MinioOps.{getFile, putFile}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.{Failure, Success}

/**
 * @author Caspar
 * @date 2022/4/11 04:34 
 */
class MinioOpsTest extends AnyFlatSpec with Matchers{
  behavior of "minio operations"

  it should "Succeed to upload onto and download file from minip" in {
    val inputPath = "./dataset/input.csv"
    val savePath = "./download"
    val putResult = MinioOps.putFile("test", "input.csv", inputPath)
    putResult match {
      case Success(_) =>
      case Failure(throwable) => fail(throwable)
    }
    val getResult = getFile("test", "input.csv", savePath, "input.csv")
    getResult match {
      case Success(_) =>
      case Failure(throwable) => fail(throwable)
    }
  }
}
