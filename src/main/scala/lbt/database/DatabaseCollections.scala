package lbt.database

import java.util.concurrent.atomic.AtomicLong

import com.mongodb.casbah.MongoCollection
import lbt.{DatabaseConfig, DefinitionsConfig}

/*
 * Database Collection Objects
 */
trait DatabaseCollections {
  val collectionName: String
  val indexKeyList: List[(String, Int)]
  val uniqueIndex: Boolean

  val db: MongoDatabase

  lazy val dBCollection: MongoCollection = db.getCollection(collectionName, indexKeyList, uniqueIndex)

  val numberInsertsRequested: AtomicLong = new AtomicLong(0)
  val numberInsertsFailed: AtomicLong = new AtomicLong(0)
  val numberInsertsCompleted: AtomicLong = new AtomicLong(0)
  val numberGetsRequested: AtomicLong = new AtomicLong(0)
  val numberDeletesRequested: AtomicLong = new AtomicLong(0)
}


