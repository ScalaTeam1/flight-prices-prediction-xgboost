package com.neu.edu.FlightPricePrediction.db

import org.mongodb.scala.{
  FindObservable,
  MongoClient,
  Observable,
  SingleObservable
}
import com.neu.edu.FlightPricePrediction.configure.Constants._
import com.neu.edu.FlightPricePrediction.db.Helpers.GenericObservable
import com.neu.edu.FlightPricePrediction.pojo.{
  Flight,
  FlightWithDate,
  TrainedModel
}
import com.typesafe.config.ConfigFactory
import org.bson.BsonDocument
import org.bson.codecs.configuration.CodecRegistries.{
  fromProviders,
  fromRegistries
}
import org.bson.conversions.Bson
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.result.{
  DeleteResult,
  InsertManyResult,
  InsertOneResult,
  UpdateResult
}

import scala.reflect.ClassTag
import scala.util.{Try, Using}
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.model.Filters.equal

object MongoDBUtils {

  val config = ConfigFactory.load(CONFIG_LOCATION)
  val mongodbConfig = config.getConfig(MONGODB_CONFIG_PREFIX)
  val username = mongodbConfig.getString(MONGODB_USERNAME)
  val passwd = mongodbConfig.getString(MONGODB_PASSWORD)
  val uri = mongodbConfig.getString(MONGODB_URI)
  val host = mongodbConfig.getString(MONGODB_HOST)
  final val DATABASE_FLIGHT_PRICE = "FlightPrice"
  final val COLLECTION_MODEL = "models"
  final val COLLECTION_FLIGHTS = "flights"

  private val codecs = fromProviders(
    classOf[Flight],
    classOf[TrainedModel],
    classOf[FlightWithDate]
  )
  private val codecRegistry = fromRegistries(codecs, DEFAULT_CODEC_REGISTRY)

  val mongodbUri = uri.format(username, passwd, host)

  def getClient = MongoClient.apply(mongodbUri)

  def getCollection[T: ClassTag](client: MongoClient, collection: String) =
    client
      .getDatabase(DATABASE_FLIGHT_PRICE)
      .getCollection[T](collection)
      .withCodecRegistry(codecRegistry)

  def exec[R, T <: Observable[R]](f: MongoClient => T) = {
    Using(getClient) { c =>
      f(c).results
    }
  }

  def execInsert(f: MongoClient => SingleObservable[InsertOneResult]) =
    exec[InsertOneResult, SingleObservable[InsertOneResult]](f)

  def execInsertMany(f: MongoClient => Observable[InsertManyResult]) =
    exec[InsertManyResult, Observable[InsertManyResult]](f)

  def execFind[T](f: MongoClient => FindObservable[T]) =
    exec[T, FindObservable[T]](f)

  def insert[T: ClassTag](
      doc: T,
      collection: String
  ): Try[Seq[InsertOneResult]] =
    execInsert(x => getCollection[T](x, collection).insertOne(doc))

  def insertMany[T: ClassTag](
      docs: Seq[T],
      collection: String
  ): Try[Seq[InsertManyResult]] = {
    execInsertMany(x => getCollection[T](x, collection).insertMany(docs))
  }

  def find[T: ClassTag](filter: Bson, collection: String): Try[Seq[T]] =
    execFind[T](x =>
      if (Nil equals filter) getCollection[T](x, collection).find()
      else getCollection[T](x, collection).find(filter)
    )

  def findByOrder[T: ClassTag](
      filter: Bson,
      order: Bson,
      limit: Int,
      collection: String
  ): Try[Seq[T]] =
    execFind[T](x =>
      if (Nil equals filter)
        getCollection[T](x, collection).find().sort(order).limit(limit)
      else getCollection[T](x, collection).find(filter).sort(order).limit(limit)
    )

  def retrieveTrainingData(num: Int) =
    findByOrder[Flight](
      new BsonDocument,
      equal("timestamp", -1),
      num,
      COLLECTION_FLIGHTS
    )

  def insertFlights(flight: Flight) = insert[Flight](flight, COLLECTION_FLIGHTS)

  def insertManyFlights(flights: Seq[Flight]) =
    insertMany[Flight](flights, COLLECTION_FLIGHTS)

  def insertFlightWithDates(flightWithDate: FlightWithDate) =
    insert[FlightWithDate](flightWithDate, COLLECTION_FLIGHTS)

  def insertManyFlightWithDates(flightWithDates: Seq[FlightWithDate]) =
    insertMany[FlightWithDate](flightWithDates, COLLECTION_FLIGHTS)

  def insertModels(fpModel: TrainedModel) =
    insert[TrainedModel](fpModel, COLLECTION_MODEL)

  def findFlights(filter: Bson) = find[Flight](filter, COLLECTION_FLIGHTS)

  def findModels(filter: Bson) = find[TrainedModel](filter, COLLECTION_MODEL)

  //delete and update
  def execUpdate(f: MongoClient => SingleObservable[UpdateResult]) =
    exec[UpdateResult, SingleObservable[UpdateResult]](f)

  def execDelete(f: MongoClient => SingleObservable[DeleteResult]) =
    exec[DeleteResult, SingleObservable[DeleteResult]](f)

  def update[T: ClassTag](
      filter: Bson,
      updateData: Bson,
      collection: String
  ): Try[Seq[UpdateResult]] = execUpdate(x =>
    getCollection[T](x, collection).updateMany(filter, updateData)
  )

  def delete[T: ClassTag](
      filter: Bson,
      collection: String
  ): Try[Seq[DeleteResult]] =
    execDelete(x => getCollection[T](x, collection).deleteMany(filter))

  def updateFlights(filter: Bson, updateData: Bson) =
    update[Flight](filter, updateData, COLLECTION_FLIGHTS)

  def updateModels(filter: Bson, updateData: Bson) =
    update[TrainedModel](filter, updateData, COLLECTION_MODEL)

  def deleteFlights(filter: Bson) = delete[Flight](filter, COLLECTION_FLIGHTS)

  def deleteModels(filter: Bson) =
    delete[TrainedModel](filter, COLLECTION_MODEL)

}
