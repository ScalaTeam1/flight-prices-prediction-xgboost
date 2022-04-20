package com.neu.edu.FlightPricePrediction.configure

/** @author Caspar
  * @date 2022/4/7 01:56
  */
object Constants {

  final val CONFIG_LOCATION = "application.conf"
  final val PREDICTOR_CONFIG_PREFIX = "com.ram.batch.predictor"
  final val TRAINER_CONFIG_PREFIX = "com.ram.batch.trainer"
  final val PERSISTENCE_CONFIG_PREFIX = "com.ram.batch.persistence"
  final val SPARK_CONFIG_PREFIX = "com.ram.batch.spark"
  final val MODEL_PATH = "model-path"
  final val PREPROCESSOR_PATH = "preprocess-model-path"
  final val APP_NAME = "app-name"

  final val SPARK_MASTER = "spark.master"
  final val SPARK_LOCAL = "local"

  final val MODEL_ID = "model-id"
  final val TRAINING_DATA_PATH = "training-data-path"
  final val INPUT_DATA_PATH = "input-data-path"
  // MongoDB
  final val MONGODB_CONFIG_PREFIX = "com.ram.batch.mongodb"
  final val MONGODB_HOST = "host"
  final val MONGODB_URI = "uri"
  final val MONGODB_PASSWORD = "password"
  final val MONGODB_USERNAME = "username"
  //S3
  final val S3_CONFIG_PREFIX = "com.ram.batch.s3"
  final val S3_ENDPOINT = "endpoint"
  final val S3_ACCESSKEY = "accessKey"
  final val S3_SECRETKEY = "secretKey"

}
