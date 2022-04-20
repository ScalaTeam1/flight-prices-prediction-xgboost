package com.neu.edu.FlightPricePrediction

import io.jvm.uuid.UUID
import ml.dmlc.xgboost4j.scala.spark.{
  TrackerConf,
  XGBoostRegressionModel,
  XGBoostRegressor
}
import org.apache.spark.ml.feature.{
  OneHotEncoder,
  StringIndexer,
  VectorAssembler
}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

object demo {
  def main(args: Array[String]): Unit = {

    val (treeMethod, numWorkers) = ("auto", 1)
    //    val (treeMethod, numWorkers) = ("gpu_hist", 1)

    val spark = SparkSession
      .builder()
      .appName("xiangdangdang")
      .config("spark.master", "local")
      .getOrCreate()
    val inputPath = "./dataset/Clean_Dataset.csv"
    val modelPath = "./model"

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
        StructField("duration", DoubleType, nullable = true),
        StructField("daysLeft", IntegerType, nullable = true),
        StructField("price", IntegerType, nullable = true)
      )
    )

    val rawInput =
      spark.read.schema(schema).option("header", value = true).csv(inputPath)
    rawInput.drop("id").drop("flight")

    val pipeline = new Pipeline()
    // pipelines
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
    val stagesArray = new ListBuffer[PipelineStage]()
    for (cate <- oneHotFeatures) {
      val indexer = new StringIndexer()
        .setInputCol(cate)
        .setOutputCol(s"${cate}Index")
        .fit(rawInput)
      val encoder = new OneHotEncoder()
        .setInputCol(indexer.getOutputCol)
        .setOutputCol(s"${cate}classVec")
      stagesArray.append(indexer, encoder)
    }

    val mappedCategoricalCols = oneHotFeatures.map(cate => s"${cate}classVec")

    // Build ML pipeline, it includes 4 stages:
    // 1, Assemble all features into a single vector column.
    // 2, From string label to indexed double label.
    // 3, Use XGBoostClassifier to train classification model.
    // 4, Convert indexed double label back to original string label.
    val assembler = new VectorAssembler()
      .setInputCols(mappedCategoricalCols ++ numericFeatures)
      .setOutputCol("features")
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
    stagesArray.append(assembler)

    pipeline.setStages(stagesArray.toArray)

    // split
    val Array(training, test) = rawInput.randomSplit(
      Array(0.8, 0.2)
    ) // Split training and test set with radio 4/1

    // Train the preprocessing model
    val uuid = UUID.random

    val preprocessModel = pipeline.fit(test)
    preprocessModel.write
      .overwrite()
      .save("./tmp/%s/preprocess_model".format(uuid))

    stagesArray.append(regressor)

    pipeline.setStages(stagesArray.toArray)

    println(s"trainingDF size=${training.count()},testDF size=${test.count()}")
    // train
    val model = pipeline.fit(training)
    val prediction = model.transform(test)
    prediction.show(false)

    val evaluator =
      new RegressionEvaluator() // Have to use regression evaluator rather than classification evaluator
    evaluator.setLabelCol("price")
    evaluator.setPredictionCol("prediction")
    val score = evaluator.evaluate(prediction)
    println("The model regression evaluation score is : " + score)

    // Tune model using cross validation
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
    // It's regression model rather than classification model
    val bestModel = cvModel.bestModel
      .asInstanceOf[PipelineModel]
      .stages(15)
      .asInstanceOf[XGBoostRegressionModel]
    println(
      "The params of best XGBoostRegressionModel : " +
        bestModel.extractParamMap()
    )
    println(
      "The training summary of best XGBoostRegressionModel : " +
        bestModel.summary
    )

    import io.jvm.uuid._
//    pipeline.write.overwrite().save("./tmp/%s/pipeline_trained".format(uuid))
    bestModel.write.overwrite().save("./tmp/%s/best_model".format(uuid))
    bestModel.nativeBooster.saveModel(modelPath)

    //    //     ML pipeline persistence
    //    model.write.overwrite().save(pipelineModelPath)
    //
    //    //     Load a saved model and serving
    //    val model2 = PipelineModel.load(pipelineModelPath)
    //    model2.transform(test).show(false)
  }
}
