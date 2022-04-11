package com.neu.edu.FlightPricePrediction.db

import org.apache.commons.io.IOUtils

import java.io.{ByteArrayInputStream, FileOutputStream}
import io.minio.MinioClient

import java.io.File
import scala.util.{Failure, Success, Try, Using}
import java.nio.file.{Files, Paths}

object MinioOps {

  // minio client with access key and secret key
  val minioClient = new MinioClient("http://localhost:9000",
    "minioadmin",
    "minioadmin")

  /**
   * Put object into minio storage
   *
   * @param bucket bucket name
   * @param id     object id
   * @param blob   object blob
   */
  def put(bucket: String, id: String, blob: Array[Byte]): Unit = {
    // create bucket if not exists
    if (!minioClient.bucketExists(bucket)) {
      minioClient.makeBucket(bucket)
    }

    // put object
    val bais = new ByteArrayInputStream(blob)
    minioClient.putObject(bucket, id, bais, bais.available(), null, null, "binary/octet-stream")

    bais.close()
  }

  /**
   * Get object from minio storage
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

  /**
   * remove object from minio storage
   *
   * @param bucket bucket name
   * @param id     object it
   */
  def delete(bucket: String, id: String): Unit = {
    // remove object
    minioClient.removeObject(bucket, id)

    // get size of the bucket
    println(List(minioClient.listObjects(bucket)).size)
  }

  def putFile(bucket: String, id: String, filePath: String) = {
    Try {
      put(bucket, id, Files.readAllBytes(Paths.get(filePath)))
    }
  }

  def getFile(bucket: String, id: String, saveDirPath: String, fileName: String) = {
    val dir = new File(saveDirPath)
    if (!dir.exists()) {
      dir.mkdir()
    }
    Using (new FileOutputStream(new File(saveDirPath + "/" + fileName))) {
      out => org.apache.commons.io.IOUtils.copy(minioClient.getObject(bucket, id), out)
    }
  }



}