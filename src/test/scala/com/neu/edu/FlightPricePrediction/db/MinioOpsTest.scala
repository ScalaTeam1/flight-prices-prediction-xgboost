package com.neu.edu.FlightPricePrediction.db

import com.neu.edu.FlightPricePrediction.db.MinioOps.minioClient
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.util.{Failure, Success}

/**
 * @author Caspar
 * @date 2022/4/11 04:34 
 */
class MinioOpsTest extends AnyFlatSpec with Matchers{
  behavior of "minio operations"

  it should "Succeed to upload onto and download file from minio" in {
    val inputPath = "./dataset/input.csv"
    val savePath = "./download"
    val putResult = MinioOps.putFile("test", "input.csv", inputPath)
    putResult match {
      case Success(_) =>
      case Failure(throwable) => fail(throwable)
    }
    val saveFile = "./download/input.csv"
    val getResult = MinioOps.getFile("test", "input.csv", savePath, "input.csv")
    getResult match {
      case Success(_) =>{
        assert(new File(saveFile).exists())
        assert(new File(saveFile).getFreeSpace()==new File(inputPath).getFreeSpace)
      }
      case Failure(throwable) => fail(throwable)
    }
  }

  it should "Succeed to delete file in minio" in {
    val deleteResult=MinioOps.deleteFile("test","input.csv")
    val savePath = "./download"
    deleteResult match {
      case Success(_)=>{
        val getResult = MinioOps.getFile("test", "input.csv", savePath, "input.csv")
        getResult should matchPattern {
          case Failure(throwable) =>
        }
      }
      case Failure(throwable) =>fail(throwable)
    }
  }
}
