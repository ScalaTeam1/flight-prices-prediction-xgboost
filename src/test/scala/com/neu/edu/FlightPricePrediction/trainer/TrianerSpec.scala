package com.neu.edu.FlightPricePrediction.trainer

import com.neu.edu.FlightPricePrediction.configure.Constants.{
  APP_NAME,
  CONFIG_LOCATION,
  SPARK_CONFIG_PREFIX,
  SPARK_LOCAL,
  SPARK_MASTER
}
import com.neu.edu.FlightPricePrediction.trainer.FlightPriceTrainer.{
  dataPath,
  loadDataNative,
  modelId,
  modelPath,
  preprocessorPath,
  spark
}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** @author Caspar
  * @date 2022/5/1 17:12
  */
class TrianerTest extends AnyFlatSpec with Matchers {

  val sparkConfig =
    ConfigFactory.load(CONFIG_LOCATION).getConfig(SPARK_CONFIG_PREFIX)

  val spark = SparkSession
    .builder()
    .appName(sparkConfig.getString(APP_NAME))
    .config(SPARK_MASTER, SPARK_LOCAL)
    .getOrCreate()

  val demoModelPath =
    "./dataset/example/5d6c3ec8-f7aa-4567-ac73-f4219bf03f85"

  val dataPath = "./dataset/Clean_Dataset.csv"

  behavior of "trainer"
  it should "work for loading local training data" in {
    val ds = loadDataNative(spark, dataPath)
    assert(ds.count() == 300153)
  }

  it should "work for training" in {
    val ds = loadDataNative(spark, dataPath)

    val trainer = FlightPriceTrainer("test", ds)

    trainer.fitAndSavePreprocessModel("./tmp/test/preprocess_model/")

    val (bestModel, score) = trainer.fitModel

    trainer.saveBestModel(bestModel, "./tmp/test/best_model/")

    assert(true)
  }
}
