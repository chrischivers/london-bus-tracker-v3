package lbt.database.definitions

import akka.actor.{Actor, ActorRef, Props}
import com.mongodb.casbah.commons.{Imports, MongoDBObject}
import com.typesafe.scalalogging.StrictLogging
import lbt.comon._
import lbt.database.definitions.BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION
import lbt.database.{IncrementNumberGetRequests, IncrementNumberInsertsCompleted, IncrementNumberInsertsRequested}


class BusDefinitionsDBSupervisor(busDefinitionsCollection: BusDefinitionsCollection) extends Actor with StrictLogging {

    val busDefinitionsDBWorker: ActorRef = context.actorOf(Props(new BusDefinitionsDBWorker(busDefinitionsCollection)), name = "BusDefinitionsDBWorker")

    override def receive: Actor.Receive = {
      case doc: (BusRoute, List[BusStop]) => busDefinitionsDBWorker ! doc
      case doc: IncrementNumberInsertsRequested => busDefinitionsCollection.numberInsertsRequested += doc.incrementBy
      case doc: IncrementNumberInsertsCompleted => busDefinitionsCollection.numberInsertsCompleted += doc.incrementBy
      case doc: IncrementNumberGetRequests => busDefinitionsCollection.numberGetRequests += doc.incrementBy
      case _ =>
        logger.error("BusDefinitionsDBSupervisor Actor received unknown message: ")
        throw new IllegalStateException("BusDefinitionsDBSupervisor received unknown message")
    }
  }


  class BusDefinitionsDBWorker(busDefinitionsCollection: BusDefinitionsCollection) extends Actor with StrictLogging {

    override def receive: Receive = {
      case doc: (BusRoute, List[BusStop]) => insertToDB(doc._1, doc._2)
      case _ =>
        logger.error("BusDefinitionsDBWorker Actor received unknown message")
        throw new IllegalStateException("BusDefinitionsDBWorker received unknown message")
    }


    private def insertToDB(busRoute: BusRoute, busStopsSequence: List[BusStop]) = {

      val stopSequenceList: List[Imports.DBObject] = busStopsSequence
          .zipWithIndex
        .map(seq => {
        MongoDBObject(
          BUS_STOP_SEQUENCE_DEFINITION.SEQUENCE_NO -> seq._2,
          BUS_STOP_SEQUENCE_DEFINITION.BUS_STOP_ID -> seq._1.id,
          BUS_STOP_SEQUENCE_DEFINITION.BUS_STOP_NAME -> seq._1.name,
          BUS_STOP_SEQUENCE_DEFINITION.LONGITUDE -> seq._1.longitude,
          BUS_STOP_SEQUENCE_DEFINITION.LATITUDE -> seq._1.latitude)

      })

      val newRouteDefDoc = MongoDBObject(
        BUS_ROUTE_DEFINITION_DOCUMENT.ROUTE_ID -> busRoute.id,
        BUS_ROUTE_DEFINITION_DOCUMENT.DIRECTION -> busRoute.direction.toString,
        BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE -> stopSequenceList)

      val query = MongoDBObject(
        BUS_ROUTE_DEFINITION_DOCUMENT.ROUTE_ID -> busRoute.id,
        BUS_ROUTE_DEFINITION_DOCUMENT.DIRECTION -> busRoute.direction.toString
      )

      busDefinitionsCollection.dBCollection.update(query, newRouteDefDoc, upsert = true)
      busDefinitionsCollection.incrementLogRequest(IncrementNumberInsertsCompleted(1))
    }

  }

case object BUS_ROUTE_DEFINITION_DOCUMENT {
  val ROUTE_ID = "ROUTE_ID"
  val DIRECTION = "DIRECTION"
  val BUS_STOP_SEQUENCE = "BUS_STOP_SEQUENCE"

  case object BUS_STOP_SEQUENCE_DEFINITION {
    val SEQUENCE_NO = "SEQUENCE_NO"
    val BUS_STOP_ID = "BUS_STOP_ID"
    val BUS_STOP_NAME = "BUS_STOP_NAME"
    val LONGITUDE = "LONGITUDE"
    val LATITUDE = "LATITUDE"
  }

}

//TODO look at serialisation of case classes