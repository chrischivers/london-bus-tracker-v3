package lbt.database

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.{Imports, MongoDBObject}
import com.typesafe.scalalogging.StrictLogging
import lbt.comon.{BusRoute, BusStop, Inbound, Outbound}
import lbt.dataSource.definitions.BusDefinitions.BusRouteDefinitions
import lbt.database.BusRouteDefinitionsDB.BUS_ROUTE_DEFINITION_DOCUMENT
import lbt.database.BusRouteDefinitionsDB.BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION

object BusRouteDefinitionsDB extends DatabaseCollections {

  implicit val system = ActorSystem("lbtSystem")

  override val dBCollection: MongoCollection = MongoDatabase.getCollection(this)

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

  override val supervisor: ActorRef = system.actorOf(Props[BusDefinitionsDBSupervisor], name = "BusDefinitionsDBSupervisor")

  def insertBusRouteDefinitionIntoDB(busRoute: BusRoute, busStops: List[BusStop]) = {
    incrementLogRequest(IncrementNumberInsertsRequested(1))
    supervisor ! (busRoute, busStops)
  }

  def getBusRouteDefinitionsFromDB: BusRouteDefinitions = {
    incrementLogRequest(IncrementNumberGetRequests(1))

    val cursor = dBCollection.find()
    cursor.map(routeDef => {
      BusRoute(
        routeDef.getAs[String](BUS_ROUTE_DEFINITION_DOCUMENT.ROUTE_ID).get,
        routeDef.getAs[String](BUS_ROUTE_DEFINITION_DOCUMENT.DIRECTION).get match {
          case "inbound" => Inbound()
          case "outbound" => Outbound()
          case other => throw new IllegalArgumentException(s"Unrecognised direction: $other")
        }) ->
        routeDef.getAs[List[DBObject]](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE).get
          .sortBy(stopDef => stopDef.getAs[Int](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION.SEQUENCE_NO))
          .map(stopDef => {
            BusStop(
              stopDef.getAs[String](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION.BUS_STOP_ID).get,
              stopDef.getAs[String](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION.BUS_STOP_NAME).get,
              stopDef.getAs[BigDecimal](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION.LATITUDE).get,
              stopDef.getAs[BigDecimal](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION.LONGITUDE).get
            )
          })
    }) toMap
  }

    override val collectionName: String = "BusRouteDefinitions"
    override val fieldsVector = Vector(BUS_ROUTE_DEFINITION_DOCUMENT.ROUTE_ID, BUS_ROUTE_DEFINITION_DOCUMENT.DIRECTION, BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE)
    override val indexKeyList = List((BUS_ROUTE_DEFINITION_DOCUMENT.ROUTE_ID, 1), (BUS_ROUTE_DEFINITION_DOCUMENT.DIRECTION, 1))
    override val uniqueIndex = true
}

  class BusDefinitionsDBSupervisor extends Actor with StrictLogging {

    val busDefinitionsDBWorker: ActorRef = context.actorOf(Props[BusDefinitionsDBWorker], name = "BusDefinitionsDBWorker")

    override def receive: Actor.Receive = {
      case doc: (BusRoute, List[BusStop]) => busDefinitionsDBWorker ! doc
      case doc: IncrementNumberInsertsRequested => BusRouteDefinitionsDB.numberInsertsRequested += doc.incrementBy
      case doc: IncrementNumberInsertsCompleted => BusRouteDefinitionsDB.numberInsertsCompleted += doc.incrementBy
      case doc: IncrementNumberGetRequests => BusRouteDefinitionsDB.numberGetRequests += doc.incrementBy
      case _ =>
        logger.error("BusDefinitionsDBSupervisor Actor received unknown message: ")
        throw new IllegalStateException("BusDefinitionsDBSupervisor received unknown message")
    }
  }


  class BusDefinitionsDBWorker extends Actor with StrictLogging {


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
        BUS_ROUTE_DEFINITION_DOCUMENT.DIRECTION -> busRoute.direction,
        BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE -> stopSequenceList)

      val query = MongoDBObject(
        BUS_ROUTE_DEFINITION_DOCUMENT.ROUTE_ID -> busRoute.id,
        BUS_ROUTE_DEFINITION_DOCUMENT.DIRECTION -> busRoute.direction
      )

      BusRouteDefinitionsDB.dBCollection.update(query, newRouteDefDoc, upsert = true)
      BusRouteDefinitionsDB.incrementLogRequest(IncrementNumberInsertsCompleted(1))
    }

  }

//TODO look at serialisation of case classes