package com.neu.edu.FlightPricePrediction.trainer

import com.neu.edu.FlightPricePrediction.configure.Constants._
import com.neu.edu.FlightPricePrediction.db.MinioOps.bucket
import com.neu.edu.FlightPricePrediction.db.{MinioOps, MongoDBUtils}
import com.neu.edu.FlightPricePrediction.pojo.{
  Flight,
  FlightReader,
  IterableFlightReader,
  TrainedModel
}
import com.neu.edu.FlightPricePrediction.trainer.FlightPriceTrainer.parentDirectory
import com.neu.edu.FlightPricePrediction.utils.FileUtil
import com.typesafe.config.ConfigFactory
import io.jvm.uuid.UUID
import ml.dmlc.xgboost4j.scala.spark.{
  TrackerConf,
  XGBoostRegressionModel,
  XGBoostRegressor
}
import org.mongodb.scala.model.Filters.equal
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.feature.{
  OneHotEncoder,
  StringIndexer,
  VectorAssembler
}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.{
  FloatType,
  IntegerType,
  StringType,
  StructField,
  StructType
}
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalDateTime
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/** @author Caspar
  * @date 2022/4/7 23:33
  */
class FlightPriceTrainer(modelId: String, ds: Dataset[Flight]) {
  val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)

  val (treeMethod, numWorkers) = ("auto", 1)

  val oneHotFeatures = Array(
    "airline",
    "sourceCity",
    "departureTime",
    "stops",
    "arrivalTime",
    "destinationCity",
    "classType"
  )
  val numericFeatures = Array("duration", "daysLeft")

  def preprocessorPipelineStages(
      oneHot: Array[String],
      numeric: Array[String]
  ) = {
    val stagesArray = new ListBuffer[PipelineStage]()
    for (cate <- oneHot) {
      val indexer = new StringIndexer()
        .setInputCol(cate)
        .setOutputCol(s"${cate}Index")
        .fit(ds)
      val encoder = new OneHotEncoder()
        .setInputCol(indexer.getOutputCol)
        .setOutputCol(s"${cate}classVec")
      stagesArray.append(indexer, encoder)
    }
    val mappedCategoricalCols = oneHot.map(cate => s"${cate}classVec")
    val assembler = new VectorAssembler()
      .setInputCols(mappedCategoricalCols ++ numeric)
      .setOutputCol("features")
    stagesArray.append(assembler)
    stagesArray
  }

  val preprocessorStages =
    preprocessorPipelineStages(oneHotFeatures, numericFeatures)

  val trackerConf = new TrackerConf(0, "scala")

  val regressor = {
    val trackerConf = new TrackerConf(0, "scala")
    val regressor = new XGBoostRegressor(
      Map(
        "eta" -> 0.1f,
        "max_depth" -> 8,
        "objective" -> "reg:squarederror",
        "num_class" -> 3,
        "num_round" -> 50,
        "num_workers" -> numWorkers,
        "tracker_conf" -> trackerConf,
        "tree_method" -> treeMethod,
        "missing" -> 0.0,
        "silent" -> 1
      )
    )

    regressor.setFeaturesCol("features")
    regressor.setLabelCol("price")
    regressor
  }

  def preprocessorPipeline = {
    val pipeline = new Pipeline()
    pipeline.setStages(preprocessorStages.toArray)
    pipeline
  }

  def getPipeline(stages: ListBuffer[PipelineStage]) = {
    val pipeline = new Pipeline()
    pipeline.setStages(stages.toArray)
    pipeline
  }

  def fitAndSavePreprocessModel(preprocessModelPath: String) = {
    logger.info("Start to fit preprocess model for $modelId")
    val pipelineModel = getPipeline(preprocessorStages).fit(ds)
    logger.info("Succeed to fit preprocess model for $modelId")
    logger.info(
      "Start to save preprocess model for $modelId at $preprocessModelPath"
    )
    pipelineModel.write.overwrite().save(preprocessModelPath)
    logger.info(
      "Succeed to save preprocess model for $modelId at $preprocessModelPath"
    )
  }

  def fitModel = {
    val Array(training, test) = ds.randomSplit(Array(0.8, 0.2))
    logger.info("Start to train regression model for $modelId")
    val predictStages = preprocessorStages.clone()
    predictStages.append(regressor)
    val pipeline = getPipeline(predictStages)
    val model = pipeline.fit(training)
    val prediction = model.transform(test)
    logger.info("Succeed to train regression model for $modelId")
    prediction.show(false)
    logger.info("Start to evaluate the regression model")

    val evaluator =
      new RegressionEvaluator() // Have to use regression evaluator rather than classification evaluator
    evaluator.setLabelCol("price")
    evaluator.setPredictionCol("prediction")
    evaluator.setMetricName("r2")
    val score = evaluator.evaluate(prediction)
    logger.info("The regression evaluation score for $modelId is : " + score)
    pipeline.getStages.toArray
    logger.info("Start to tune the regression model $modelId")

    val paramGrid = new ParamGridBuilder()
      .addGrid(regressor.maxDepth, Array(3, 8))
      .addGrid(regressor.eta, Array(0.2, 0.6))
      .build()
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    val cvModel = cv.fit(training)
    val bestModel = cvModel.bestModel
      .asInstanceOf[PipelineModel]
      .stages(15)
      .asInstanceOf[XGBoostRegressionModel]
    logger.info(
      "The params of best XGBoostRegressionModel $modelId : " +
        bestModel.extractParamMap()
    )
    logger.info(
      "The training summary of best XGBoostRegressionModel $modelId: " +
        bestModel.summary
    )
    (bestModel, score)
  }

  def saveBestModel(model: XGBoostRegressionModel, path: String) = {
    logger.info(s"Start to save regression model for $modelId in $path")
    model.write.overwrite().save(path)
    logger.info(s"Succeed to save regression model for $modelId in $path")
  }

  def persistence(score: Double) = {
    FileUtil.zip(parentDirectory, parentDirectory + ".zip")
    MinioOps.putFile(bucket, modelId + ".zip", parentDirectory + ".zip")
    MongoDBUtils.insertModels(
      TrainedModel(
        modelId,
        "./preprocess_model",
        "./best_model",
        score,
        LocalDateTime.now()
      )
    )
  }

}

object FlightPriceTrainer extends App {

  val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)

  def apply(modelId: String, ds: Dataset[Flight]) =
    new FlightPriceTrainer(modelId, ds)

  val modelId: String = UUID.random.toString
  logger.info(s"Start to train model modelId: $modelId")
  val config = ConfigFactory.load(CONFIG_LOCATION)
  val trainerConfig = config.getConfig(TRAINER_CONFIG_PREFIX)
  val trainingSetSize = trainerConfig.getInt(TRAINING_SET_SIZE)
  val persistenceConfig = config.getConfig(PERSISTENCE_CONFIG_PREFIX)
  val modelPath = persistenceConfig.getString(MODEL_PATH).format(modelId)
  val parentDirectory =
    persistenceConfig.getString(PARENT_DIRECTORY).format(modelId)
  val dataPath = persistenceConfig.getString(TRAINING_DATA_PATH)
  val preprocessorPath = {
    persistenceConfig.getString(PREPROCESSOR_PATH).format(modelId)
  }
  val sparkConfig =
    ConfigFactory.load(CONFIG_LOCATION).getConfig(SPARK_CONFIG_PREFIX)

  val spark = SparkSession
    .builder()
    .appName(sparkConfig.getString(APP_NAME))
    .config(SPARK_MASTER, SPARK_LOCAL)
    .getOrCreate()

  def retrieveDataFromMongoDB(): Try[Dataset[Flight]] = {
    MongoDBUtils.retrieveTrainingData(trainingSetSize) map { x =>
      IterableFlightReader(x).dy
    }
  }

  def loadDataNative(spark: SparkSession, dataPath: String) = {
    val schema = new StructType(
      Array(
        StructField("id", IntegerType, nullable = true),
        StructField("airline", StringType, nullable = true),
        StructField("flight", StringType, nullable = true),
        StructField("sourceCity", StringType, nullable = true),
        StructField("departureTime", StringType, nullable = true),
        StructField("stops", StringType, nullable = true),
        StructField("arrivalTime", StringType, nullable = true),
        StructField("destinationCity", StringType, nullable = true),
        StructField("classType", StringType, nullable = true),
        StructField("duration", FloatType, nullable = true),
        StructField("daysLeft", IntegerType, nullable = true),
        StructField("price", IntegerType, nullable = true)
      )
    )
    import spark.implicits._
    val df =
      spark.read.schema(schema).option("header", value = true).csv(dataPath)
    df.drop("id").drop("flight")
    val ds = df.as[Flight]
    ds
  }

  def loadDataTableParser(dataPath: String): Dataset[Flight] = {
    logger.info(s"Start to parse csv file $dataPath")
    val ds = FlightReader(dataPath).dy.get
    logger.info(s"Succeed to parse csv file $dataPath")
    ds
  }

//  val ds = loadDataNative(spark, dataPath)
//  val ds = loadDataTableParser(dataPath)
  val ds = retrieveDataFromMongoDB.get

  val trainer = FlightPriceTrainer(modelId, ds)

  trainer.fitAndSavePreprocessModel(preprocessorPath)

  val (bestModel, score) = trainer.fitModel

  trainer.saveBestModel(bestModel, modelPath)

  trainer.persistence(score)

}
