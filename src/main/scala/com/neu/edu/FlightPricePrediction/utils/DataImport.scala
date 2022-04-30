package com.neu.edu.FlightPricePrediction.utils

import com.neu.edu.FlightPricePrediction.configure.Constants.{
  APP_NAME,
  CONFIG_LOCATION,
  PERSISTENCE_CONFIG_PREFIX,
  SPARK_CONFIG_PREFIX,
  SPARK_LOCAL,
  SPARK_MASTER,
  TRAINING_DATA_PATH
}
import com.neu.edu.FlightPricePrediction.db.MongoDBUtils
import com.neu.edu.FlightPricePrediction.pojo.{Flight, FlightWithDate}
import com.neu.edu.FlightPricePrediction.trainer.FlightPriceTrainer
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/** @author Caspar
  * @date 2022/4/26 21:17
  */
object DataImport extends App {
  val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)

  val config = ConfigFactory.load(CONFIG_LOCATION)
  val persistenceConfig = config.getConfig(PERSISTENCE_CONFIG_PREFIX)
  val dataPath = persistenceConfig.getString(TRAINING_DATA_PATH)
  val sparkConfig = config.getConfig(SPARK_CONFIG_PREFIX)
  val spark = SparkSession
    .builder()
    .appName(sparkConfig.getString(APP_NAME))
    .config(SPARK_MASTER, SPARK_LOCAL)
    .getOrCreate()
  logger.info(s"Start to read data from local csv files: $dataPath.")
  val ds: Dataset[Flight] = FlightPriceTrainer.loadDataNative(spark, dataPath)
  logger.info(s"Ready to insert ${ds.collect().size} data points!")
  logger.info(s"Succeed to load data from local csv files: $dataPath.")
  val flightWithDates: Seq[FlightWithDate] =
    ds.collect().map(FlightWithDate.toFlightWithDate(_)).toSeq
  val res = MongoDBUtils.insertManyFlightWithDates(flightWithDates)
  logger.info("Ok")
}
