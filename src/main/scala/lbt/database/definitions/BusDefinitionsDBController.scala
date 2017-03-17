package lbt.database.definitions

import com.mongodb.casbah.Imports.{DBObject, _}
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.{Imports, MongoDBObject}
import com.typesafe.scalalogging.StrictLogging
import lbt.comon.Commons.BusRouteDefinitions
import lbt.comon._
import lbt.database.definitions.BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future


object BusDefinitionsDBController extends StrictLogging {

  def insertRouteIntoDB(col: MongoCollection, busRoute: BusRoute, busStopsSequence: List[BusStop]): Future[Boolean] = {

    Future {
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

      col.update(query, newRouteDefDoc, upsert = true).wasAcknowledged()
    }
  }

  def loadBusRouteDefinitionsFromDB(col: MongoCollection): BusRouteDefinitions = {
    logger.info("Loading Bus Route Definitions From DB")
      val cursor = col.find()
      cursor.map(routeDef => {
        BusRoute(
          routeDef.getAs[String](BUS_ROUTE_DEFINITION_DOCUMENT.ROUTE_ID).get,
          routeDef.getAs[String](BUS_ROUTE_DEFINITION_DOCUMENT.DIRECTION).get) ->
          routeDef.getAs[List[DBObject]](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE).get
            .sortBy(stopDef => stopDef.getAs[Int](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION.SEQUENCE_NO))
            .map(stopDef => {
              BusStop(
                stopDef.getAs[String](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION.BUS_STOP_ID).get,
                stopDef.getAs[String](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION.BUS_STOP_NAME).get,
                stopDef.getAs[Double](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION.LATITUDE).get,
                stopDef.getAs[Double](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION.LONGITUDE).get
              )
            })
      }) toMap
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