package com.neu.edu.FlightPricePrediction.db

import com.neu.edu.FlightPricePrediction.db.MongoDBUtils.COLLECTION_FLIGHTS
import com.neu.edu.FlightPricePrediction.pojo.FlightWithDate.toFlightWithDate
import com.neu.edu.FlightPricePrediction.pojo.{Flight, FlightWithDate}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Success

/** @author Caspar
  * @date 2022/4/10 18:08
  */
class MongoDBUtilsTest extends AnyFlatSpec with Matchers {

  behavior of "mongodb insert"
  val flight1 = Flight(
    1,
    "airline",
    "flight",
    "sourcecity",
    "departureTIme",
    "stops",
    "arrivalTime",
    "destinationCity",
    "class",
    2.1d,
    1,
    400
  )
  val flight2 = Flight(
    2,
    "airline",
    "flight",
    "sourcecity",
    "departureTIme",
    "stops",
    "arrivalTime",
    "destinationCity",
    "class",
    2.1d,
    1,
    400
  )
  it should "work for insert into collection Flights" in {
    val insertRes = MongoDBUtils.insertFlights(flight1)
    insertRes match {
      case Success(_) => {
        val flights = MongoDBUtils.findFlights(
          org.mongodb.scala.model.Filters.equal("id", 1)
        )
        assert(flights.get.last.flight.equals("flight"))
      }
    }
  }

  it should "work for insert FlightWithDate into collection Flights" in {
    val insertRes =
      MongoDBUtils
        .insert[FlightWithDate](
          toFlightWithDate(flight1),
          COLLECTION_FLIGHTS
        )
    insertRes match {
      case Success(_) => {
        val flights = MongoDBUtils.findFlights(
          org.mongodb.scala.model.Filters.equal("id", 1)
        )
        assert(flights.get.last.flight.equals("flight"))
      }
    }
  }

  it should "work for insert FlightWithDate sequences into collection Flights" in {
    val insertRes =
      MongoDBUtils
        .insertManyFlightWithDates(
          Seq(flight1, flight2).map(e => toFlightWithDate(e))
        )
    insertRes match {
      case Success(_) => {
        val flights = MongoDBUtils.findFlights(
          org.mongodb.scala.model.Filters.equal("id", 1)
        )
        assert(flights.get.last.flight.equals("flight"))
      }
    }
  }

  it should "work for query from collection Flights" in {
    val res =
      MongoDBUtils.findFlights(org.mongodb.scala.model.Filters.equal("id", 1))
    res match {
      case Success(x) => {
        assert(res.get.last.flight.equals("flight"))
      }
    }
  }

  it should "work for update from collection Flights" in {
    val res = MongoDBUtils.updateFlights(
      org.mongodb.scala.model.Filters.equal("id", 1),
      org.mongodb.scala.model.Updates.set("flight", "updateFlight")
    )
    res match {
      case Success(x) => {
        val flights = MongoDBUtils.findFlights(
          org.mongodb.scala.model.Filters.equal("id", 1)
        )
        assert(flights.get.last.flight.equals("updateFlight"))
      }
    }
  }

  it should "work for delete from collection Flights" in {
    val res =
      MongoDBUtils.deleteFlights(org.mongodb.scala.model.Filters.equal("id", 1))
    res match {
      case Success(x) => {
        val flights = MongoDBUtils.findFlights(
          org.mongodb.scala.model.Filters.equal("id", 1)
        )
        assert(flights.get.size == 0)
      }
    }
  }

  it should "work for retrieving training data" in {
    val res = MongoDBUtils.retrieveTrainingData(100)
    res match {
      case Success(x) => {
        assert(true)
      }
    }
  }
}
