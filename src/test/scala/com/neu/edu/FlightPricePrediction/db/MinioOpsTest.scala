package com.neu.edu.FlightPricePrediction.db

import com.neu.edu.FlightPricePrediction.db.MinioOps.minioClient
import org.scalatest.TryValues.convertTryToSuccessOrFailure
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.util
import scala.util.{Failure, Success}

/**
  * @author Caspar
  * @date 2022/4/11 04:34
  */
class MinioOpsTest extends AnyFlatSpec with Matchers {
	behavior of "minio operations"

	it should "Succeed to upload onto and download file from minio" in {
		val inputPath = "./dataset/input.csv"
		val savePath = "./download"
		val putResult = MinioOps.putFile("test", "input.csv", inputPath)
		val saveFile = "./download/input.csv"
		val getResult = MinioOps.getFile("test", "input.csv", savePath, "input.csv")
		getResult match {
			case Success(_) => {
				assert(new File(saveFile).exists())
				assert(new File(saveFile).getFreeSpace() == new File(inputPath).getFreeSpace)
			}
		}
	}

	it should "Fail to find file in upload" in {
		val inputPath = "./dataset/input2.csv"
		val putResult = MinioOps.putFile("test", "input.csv", inputPath)
		putResult.failure.exception shouldBe a[MinioFileException]
	}

	it should "Fail to find file in download" in {
		val savePath = "./download"
		val getResult = MinioOps.getFile("test", "input44.csv", savePath, "input.csv")
		getResult.failure.exception shouldBe a[MinioBucketFileNotFoundException]
	}

	it should "Fail to find bucket in download" in {
		val savePath = "./download"
		val getResult = MinioOps.getFile("test66", "input.csv", savePath, "input.csv")
		getResult.failure.exception shouldBe a[MinioBucketException]
	}

	it should "Succeed to delete file in minio" in {
		val deleteResult = MinioOps.deleteFile("test", "input.csv")
		val savePath = "./download"
		deleteResult match {
			case Success(_) => {
				val getResult = MinioOps.getFile("test", "input.csv", savePath, "input.csv")
				getResult.failure.exception shouldBe a[MinioBucketFileNotFoundException]
			}
		}
	}

	it should "fail to find bucket in delete" in {
		val deleteResult = MinioOps.deleteFile("test5", "input.csv")
		deleteResult.failure.exception shouldBe a[MinioBucketException]
	}
}
