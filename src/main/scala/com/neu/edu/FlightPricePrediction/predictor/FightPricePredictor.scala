package com.neu.edu.FlightPricePrediction.predictor

import com.neu.edu.FlightPricePrediction.configure.Constants._
import com.neu.edu.FlightPricePrediction.db.MongoDBUtils
import com.neu.edu.FlightPricePrediction.pojo.{Flight, FlightReader}
import com.typesafe.config.ConfigFactory
import ml.dmlc.xgboost4j.scala.spark.XGBoostRegressionModel
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.joda.time.DateTime

import scala.util.Try
/**
 * @author Caspar
 * @date 2022/4/4 23:20
 */

case class FightPricePredictor(modelId: String, model: XGBoostRegressionModel, preprocessor: PipelineModel) extends Predictor[Flight] {
  def predict(data: Try[Dataset[Flight]]): Try[DataFrame] = {
    for (ds <- data) yield model.transform(preprocessor.transform(ds))
  }
}

object FightPricePredictor extends App {

  def apply(modelId: String, model: XGBoostRegressionModel, preprocessorModel: PipelineModel) = new FightPricePredictor(modelId, model, preprocessorModel)

  def loadModel(path: String) = XGBoostRegressionModel.load(path)
  def loadPreprocessModel(path: String): PipelineModel = PipelineModel.load(path)
  val config = ConfigFactory.load(CONFIG_LOCATION)
  val predictorConfig = config.getConfig(PREDICTOR_CONFIG_PREFIX)
  val modelId = predictorConfig.getString(MODEL_ID)

  val persistenceConfig = config.getConfig(PERSISTENCE_CONFIG_PREFIX)
  val modelPath = persistenceConfig.getString(MODEL_PATH).format(modelId)
  val dataPath = persistenceConfig.getString(INPUT_DATA_PATH)
  val preprocessorPath = persistenceConfig.getString(PREPROCESSOR_PATH).format(modelId)

  val sparkConfig = ConfigFactory.load(CONFIG_LOCATION).getConfig(SPARK_CONFIG_PREFIX)

  val spark = SparkSession
    .builder()
    .appName(sparkConfig.getString(APP_NAME))
    .config(SPARK_MASTER, SPARK_LOCAL)
    .getOrCreate()


  val predictor = apply(modelId, loadModel(modelPath), loadPreprocessModel(preprocessorPath))

  val input = FlightReader(dataPath)

  val output = predictor.predict(input.dy)

  output foreach (x => x.show())
}
