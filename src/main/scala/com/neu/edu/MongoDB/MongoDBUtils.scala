package com.neu.edu.MongoDB

import org.mongodb.scala.{Document, MongoClient, MongoDatabase}
import com.neu.edu.MongoDB.Helpers._

object MongoDBUtils {
  val uri: String = "mongodb+srv://YuhanYang:Yaqi1830.yyh@scala.22epm.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
  val client=MongoClient(uri)
  def getCollection(CollectionName: String)=client.getDatabase("Scala").getCollection(CollectionName)
  def insertIntoCollection(doc: Document, collectionName: String)= getCollection(collectionName).insertOne(doc).results()
  def close()= client.close()
}
