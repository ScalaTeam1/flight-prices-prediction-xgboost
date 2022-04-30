package com.neu.edu.FlightPricePrediction.predictor

import com.neu.edu.FlightPricePrediction.configure.Constants._
import com.neu.edu.FlightPricePrediction.pojo.FlightReader
import com.neu.edu.FlightPricePrediction.predictor.FightPricePredictor.{
  loadModel,
  loadPreprocessModel
}
import com.neu.edu.FlightPricePrediction.utils.FileUtil
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.util.Success

/** @author Caspar
  * @date 2022/4/30 17:23
  */

class PredictorTest extends AnyFlatSpec with Matchers {
  val sparkConfig =
    ConfigFactory.load(CONFIG_LOCATION).getConfig(SPARK_CONFIG_PREFIX)

  val spark = SparkSession
    .builder()
    .appName(sparkConfig.getString(APP_NAME))
    .config(SPARK_MASTER, SPARK_LOCAL)
    .getOrCreate()

  val demoModelPath =
    "./dataset/example/5d6c3ec8-f7aa-4567-ac73-f4219bf03f85"

  behavior of "predictor"

  it should "work for unzip" in {
    val target = "./dataset/example/5d6c3ec8-f7aa-4567-ac73-f4219bf03f85"
    FileUtil.unzip(demoModelPath + ".zip", target)
    val file = new File(target)
    assert(file.exists())
  }

  it should "work for parse input" in {
    val data = FlightReader("./dataset/input_example.csv").dy
    assert(data.get.count() == 10)
  }

  it should "work for loading model and preprocess model" in {
    val model = loadModel(demoModelPath + "/best_model")
    val preprocess_model =
      loadPreprocessModel(demoModelPath + "/preprocess_model")
    assert(true)
  }

  it should "work for predicting" in {
    val predictor = FightPricePredictor(
      "demoPredictor",
      loadModel(demoModelPath + "/best_model"),
      loadPreprocessModel(demoModelPath + "/preprocess_model")
    )
    val tdf = predictor.predict(FlightReader("./dataset/input_example.csv").dy)
    tdf match {
      case Success(_) =>
    }
  }

}
