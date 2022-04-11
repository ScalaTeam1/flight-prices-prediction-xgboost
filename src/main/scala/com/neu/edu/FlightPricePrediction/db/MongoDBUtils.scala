package com.neu.edu.FlightPricePrediction.db

import org.mongodb.scala.{FindObservable, MongoClient, Observable, SingleObservable}
import com.neu.edu.FlightPricePrediction.configure.Constants._
import com.neu.edu.FlightPricePrediction.db.Helpers.GenericObservable
import com.neu.edu.FlightPricePrediction.pojo.{Flight, TrainedModel}
import com.typesafe.config.ConfigFactory
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.result.InsertOneResult

import scala.reflect.ClassTag
import scala.util.{Try, Using}
import org.mongodb.scala.bson.codecs.Macros._

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

  private val codecs = fromProviders(classOf[Flight], classOf[TrainedModel])
  private val codecRegistry = fromRegistries(codecs, DEFAULT_CODEC_REGISTRY)

  val mongodbUri = uri.format(username, passwd, host)

  def getClient = MongoClient.apply(mongodbUri)

  def getCollection[T: ClassTag](client: MongoClient, collection: String) = client.getDatabase(DATABASE_FLIGHT_PRICE).getCollection[T](collection).withCodecRegistry(codecRegistry)

  def exec[R, T<: Observable[R]](f: MongoClient => T) = {
    Using (getClient) {
      c => f(c).results
    }
  }

  def execInsert(f: MongoClient => SingleObservable[InsertOneResult]) = exec[InsertOneResult, SingleObservable[InsertOneResult]](f)

  def execFind[T](f: MongoClient => FindObservable[T]) = exec[T, FindObservable[T]](f)

  def insert[T: ClassTag](doc: T, collection: String): Try[Seq[InsertOneResult]] = execInsert(x => getCollection[T](x, collection).insertOne(doc))

  def find[T: ClassTag](filter: Bson, collection: String): Try[Seq[T]] = execFind[T](x => if (Nil equals filter) getCollection[T](x, collection).find() else getCollection[T](x, collection).find(filter))

  def insertFlights(flight: Flight) = insert[Flight](flight, COLLECTION_FLIGHTS)

  def insertModels(fpModel: TrainedModel) = insert[TrainedModel](fpModel, COLLECTION_MODEL)

  def findFlights(filter: Bson) = find[Flight](filter, COLLECTION_FLIGHTS)

  def findModels(filter: Bson) = find[TrainedModel](filter, COLLECTION_MODEL)

}