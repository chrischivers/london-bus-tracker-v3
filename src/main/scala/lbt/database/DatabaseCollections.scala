package lbt.database

import akka.actor.ActorRef
import com.mongodb.casbah.MongoCollection

/*
 * Database Collection Objects
 */
trait DatabaseCollections {
  val collectionName: String
  val fieldsVector: Vector[String]
  val indexKeyList: List[(String,Int)]
  val uniqueIndex: Boolean

  val dBCollection: MongoCollection

  val supervisor: ActorRef

  def incrementLogRequest(ilv: IncrementLogValues) = supervisor ! ilv

  var numberInsertsRequested:Long  = 0
  var numberInsertsCompleted:Long  = 0
  var numberGetRequests:Long = 0
  var numberDeleteRequests:Long = 0

}


trait IncrementLogValues

case class IncrementNumberInsertsRequested(incrementBy: Int) extends IncrementLogValues
case class IncrementNumberInsertsCompleted(incrementBy: Int) extends IncrementLogValues
case class IncrementNumberGetRequests(incrementBy: Int) extends IncrementLogValues
case class IncrementNumberDeleteRequests(incrementBy: Int) extends IncrementLogValues

