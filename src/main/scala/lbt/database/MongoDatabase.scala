package lbt.database

import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import com.typesafe.scalalogging.StrictLogging
import lbt.{ConfigLoader, DatabaseConfig}

class MongoDatabase(dbConfig: DatabaseConfig) extends StrictLogging {

  val dBName: String = dbConfig.databaseName

  val client = MongoClient()
  val db = client(dBName)

  def getCollection(collectionName: String, indexKeyList: List[(String, Int)], unique: Boolean): MongoCollection = {
    logger.info(s"Getting collection for colName: $collectionName")
    val col = db(collectionName)
    setIndex(collectionName, indexKeyList, unique)
    col
  }

  def closeConnection() = client.close()

  def dropDatabase = {
    db.dropDatabase()
  }

  private def setIndex(collectionName: String, indexKeyList: List[(String, Int)], unique: Boolean) = {
    logger.info(s"Setting index for collection $collectionName with indexKeyList: $indexKeyList")
    val col = db.getCollection(collectionName)
    col.createIndex(MongoDBObject(indexKeyList),MongoDBObject("unique" -> unique))
  }
}

object MongoDatabase {
  private val defaultDbConfig = ConfigLoader.defaultConfig.databaseConfig

  def apply(): MongoDatabase = apply(defaultDbConfig)

  def apply(dbConfig: DatabaseConfig): MongoDatabase = new MongoDatabase(dbConfig)

}
