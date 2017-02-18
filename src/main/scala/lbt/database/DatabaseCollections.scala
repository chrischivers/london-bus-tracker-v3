package lbt.database

import com.mongodb.casbah.MongoCollection
import lbt.{DatabaseConfig, DefinitionsConfig}

/*
 * Database Collection Objects
 */
trait DatabaseCollections {
  val collectionName: String
  val fieldsVector: Vector[String]
  val indexKeyList: List[(String, Int)]
  val uniqueIndex: Boolean

  val db: MongoDatabase

  lazy val dBCollection: MongoCollection = db.getCollection(collectionName, indexKeyList, uniqueIndex)

  var numberInsertsRequested: Long = 0
  var numberInsertsCompleted: Long = 0
  var numberGetRequests: Long = 0
  var numberDeleteRequests: Long = 0


  def incrementLogRequest(ilv: IncrementLogValues) = ilv match {
    case IncrementNumberInsertsRequested(n) => numberInsertsRequested += n
    case IncrementNumberInsertsCompleted(n) => numberInsertsCompleted += n
    case IncrementNumberGetRequests(n) => numberGetRequests += n
    case IncrementNumberDeleteRequests(n) => numberDeleteRequests += n
  }
}

trait IncrementLogValues

case class IncrementNumberInsertsRequested(incrementBy: Int) extends IncrementLogValues

case class IncrementNumberInsertsCompleted(incrementBy: Int) extends IncrementLogValues

case class IncrementNumberGetRequests(incrementBy: Int) extends IncrementLogValues

case class IncrementNumberDeleteRequests(incrementBy: Int) extends IncrementLogValues

