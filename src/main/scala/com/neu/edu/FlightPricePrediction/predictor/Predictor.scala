package com.neu.edu.FlightPricePrediction.predictor

import org.apache.spark.sql.{DataFrame, Dataset}

import scala.util.Try

/**
 * @author Caspar
 * @date 2022/4/7 21:43 
 */
trait Predictor[T] {

  def predict(data: Try[Dataset[T]]): Try[DataFrame]
}
