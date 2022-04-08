package com.neu.edu.MongoDB

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.{AmazonS3Client, AmazonS3ClientBuilder}
import org.apache.commons.io.IOUtils

import java.io.{File, FileOutputStream}

object S3Utils {
  val BUCKET_NAME = "scala-bucket-yyh"
  val AWS_ACCESS_KEY = ""
  val AWS_SECRET_KEY = ""
  val awsCredentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY)
  val amazonS3Client = AmazonS3ClientBuilder
    .standard()
    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
    .build()

  def upload(filePath :String, fileName :String)={
    val file=new File(filePath)
    amazonS3Client.putObject(BUCKET_NAME,fileName,file)
  }

  def download(fileName :String, savingPath:String)={
    val obj=amazonS3Client.getObject(BUCKET_NAME,fileName)
    val bytes = IOUtils.toByteArray(obj.getObjectContent())
    val file = new FileOutputStream(new File(savingPath))
    file.write(bytes)
  }
}
