package com.neu.edu.FlightPricePrediction.pojo

import java.time.LocalDateTime

/** @author Caspar
  * @date 2022/4/26 00:04
  */
case class FlightWithDate(
    id: Int,
    airline: String,
    flight: String,
    sourceCity: String,
    departureTime: String,
    stops: String,
    arrivalTime: String,
    destinationCity: String,
    classType: String,
    duration: Double,
    daysLeft: Int,
    price: Int,
    timestamp: LocalDateTime
)

object FlightWithDate {
  def apply(flight: Flight, date: LocalDateTime): FlightWithDate =
    (flight, date) match {
      case (_, _) =>
        FlightWithDate(
          flight.id,
          flight.airline,
          flight.flight,
          flight.sourceCity,
          flight.departureTime,
          flight.stops,
          flight.arrivalTime,
          flight.destinationCity,
          flight.classType,
          flight.duration,
          flight.daysLeft,
          flight.price,
          date
        )
    }
  def toFlightWithDate(flight: Flight): FlightWithDate = {
    apply(flight, LocalDateTime.now())
  }

}
