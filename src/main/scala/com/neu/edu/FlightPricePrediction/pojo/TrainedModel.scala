package com.neu.edu.FlightPricePrediction.pojo

import java.time.LocalDateTime

/** @author Caspar
  * @date 2022/4/10 02:57
  */
case class TrainedModel(
    uuid: String,
    pipelineModelPath: String,
    regressionModelPath: String,
    score: Double,
    datetime: LocalDateTime
)
