package lbt.database

import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import com.typesafe.scalalogging.StrictLogging

object MongoDatabase extends StrictLogging with App{

  val dBName: String = "LbtDB"

  val client = MongoClient()
  val db = client(dBName)

  getCollection(BusRouteDefinitionsDB)

  def getCollection(dbc:DatabaseCollections): MongoCollection = {
    val coll = db(dbc.collectionName)
    createIndex(coll, dbc)
    logger.info("Index Info: " + coll.getIndexInfo)
    coll
  }

 // def closeConnection() = client.close()

  def createIndex(mongoCollection: MongoCollection, dbc: DatabaseCollections) = {
    if (dbc.uniqueIndex) mongoCollection.createIndex(MongoDBObject(dbc.indexKeyList),MongoDBObject("unique" -> true))
    else mongoCollection.createIndex(MongoDBObject(dbc.indexKeyList),MongoDBObject("unique" -> false))
  }
}
