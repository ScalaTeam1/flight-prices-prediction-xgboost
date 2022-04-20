package com.neu.edu.FlightPricePrediction.db

import com.neu.edu.FlightPricePrediction.configure.Constants.{
  CONFIG_LOCATION,
  S3_ACCESSKEY,
  S3_CONFIG_PREFIX,
  S3_ENDPOINT,
  S3_SECRETKEY
}
import com.typesafe.config.ConfigFactory
import org.apache.commons.io.IOUtils

import java.io.{ByteArrayInputStream, File, FileOutputStream}
import io.minio.MinioClient
import io.minio.errors.{ErrorResponseException}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try, Using}
import java.nio.file.{Files, NoSuchFileException, Paths}

object MinioOps {

  val config = ConfigFactory.load(CONFIG_LOCATION)
  val s3Config = config.getConfig(S3_CONFIG_PREFIX)
  val endpoint = s3Config.getString(S3_ENDPOINT)
  val accessKey = s3Config.getString(S3_ACCESSKEY)
  val secretKey = s3Config.getString(S3_SECRETKEY)

  // minio client with access key and secret key
  val minioClient = new MinioClient(endpoint, accessKey, secretKey)

  val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)

  /** Put object into minio storage
    *
    * @param bucket bucket name
    * @param id     object id
    * @param blob   object blob
    */
  def put(bucket: String, id: String, blob: Array[Byte]): Unit = {
    // create bucket if not exists
    if (!minioClient.bucketExists(bucket)) {
      minioClient.makeBucket(bucket)
      logger.info(s"Bucket: $bucket created since it is not exist")
    }

    // put object
    val bais = new ByteArrayInputStream(blob)
    minioClient.putObject(
      bucket,
      id,
      bais,
      bais.available(),
      null,
      null,
      "binary/octet-stream"
    )

    bais.close()
  }

  /** Get object from minio storage
    *
    * @param bucket bucket name
    * @param id     object it
    */
  def get(bucket: String, id: String): Unit = {
    // get object as byte array
    val stream = minioClient.getObject(bucket, id)
    val blob = IOUtils.toByteArray(stream)
    println(blob.length)

    // get object stat
    val stat = minioClient.statObject(bucket, id)
    println(stat.bucketName())
  }

  /** remove object from minio storage
    *
    * @param bucket bucket name
    * @param id     object it
    */
  def delete(bucket: String, id: String): Unit = {
    // remove object
    minioClient.removeObject(bucket, id)
  }

  def putFile(bucket: String, id: String, filePath: String) = {
    Try {
      put(bucket, id, Files.readAllBytes(Paths.get(filePath)))
    } match {
      case Success(a) => Success(a)
      case Failure(e: NoSuchFileException) => {
        logger.error(s"Upload Fail. File not found at $filePath")
        Failure(MinioFileException(filePath, e))
      }
      case Failure(exception: Exception) => {
        logger.error(
          s"Upload Fail. Unknown Exception with message: ${exception.getMessage}"
        )
        Failure(exception)
      }
    }
  }

  def getFile(
      bucket: String,
      id: String,
      saveDirPath: String,
      fileName: String
  ) = {
    val dir = new File(saveDirPath)
    if (!dir.exists()) {
      dir.mkdirs()
    }
    if (!minioClient.bucketExists(bucket)) {
      logger.error(s"Download Fail. Bucket $bucket not exist")
      Failure(MinioBucketException(bucket))
    } else {
      Using(new FileOutputStream(new File(saveDirPath + "/" + fileName))) {
        out =>
          org.apache.commons.io.IOUtils
            .copy(minioClient.getObject(bucket, id), out)
      } match {
        case Success(value) => Success(value)
        case Failure(exception: ErrorResponseException) => {
          logger.error(s"Download Fail. File $id not exist in Bucket $bucket")
          Failure(MinioBucketFileNotFoundException(bucket, id, exception))
        }
        case Failure(exception: Exception) => {
          logger.error(
            s"Download Fail with unknown exception. Message:${exception.getMessage}"
          )
          Failure(exception)
        }
      }
    }
  }

  def deleteFile(bucket: String, id: String) = {
    if (!minioClient.bucketExists(bucket)) {
      logger.error(s"Delete Fail. Bucket $bucket not exist")
      Failure(MinioBucketException(bucket))
    } else {
      Try { delete(bucket, id) } match {
        case Success(a) => Success(a)
        case Failure(e) => {
          logger.error("Delete Failed")
          Failure(e)
        }
      }
    }
  }

}

case class MinioFileException(filePath: String, cause: Throwable)
    extends Exception(s"File not found from this path: $filePath", cause)

case class MinioBucketException(bucketName: String)
    extends Exception(s"Bucket not exist with name $bucketName")

case class MinioBucketFileNotFoundException(
    bucketName: String,
    fileName: String,
    cause: Throwable
) extends Exception(s"File $fileName not exist in Bucket $bucketName", cause)
