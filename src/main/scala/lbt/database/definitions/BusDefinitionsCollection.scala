package lbt.database.definitions

import com.mongodb.casbah.Imports.{DBObject, _}
import com.mongodb.casbah.MongoCollection
import com.typesafe.scalalogging.StrictLogging
import lbt.comon.Commons.BusRouteDefinitions
import lbt.comon.{BusRoute, BusStop, Commons}
import lbt.database._
import lbt.{ConfigLoader, DatabaseConfig, DefinitionsConfig}
import scala.concurrent.ExecutionContext.Implicits.global

import scala.util.{Failure, Success}

class BusDefinitionsCollection(val defConfig: DefinitionsConfig, val dbConfig: DatabaseConfig) extends DatabaseCollections with StrictLogging{

  override val db: MongoDatabase = new MongoDatabase(dbConfig)
  override val collectionName: String = defConfig.dBCollectionName
  override val fieldsVector = Vector(BUS_ROUTE_DEFINITION_DOCUMENT.ROUTE_ID, BUS_ROUTE_DEFINITION_DOCUMENT.DIRECTION, BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE)
  override val indexKeyList = List((BUS_ROUTE_DEFINITION_DOCUMENT.ROUTE_ID, 1), (BUS_ROUTE_DEFINITION_DOCUMENT.DIRECTION, 1))
  override val uniqueIndex = true


  def insertBusRouteDefinitionIntoDB(busRoute: BusRoute, busStops: List[BusStop]) = {
    incrementLogRequest(IncrementNumberInsertsRequested(1))
    BusDefinitionsDBController.insertRouteIntoDB(dBCollection, busRoute, busStops).onComplete {
      case Success(ack) =>  if (ack) incrementLogRequest(IncrementNumberInsertsCompleted(1))
                            else logger.info(s"Insert Bus Route Definition for route $busRoute was not acknowledged by DB")
      case Failure(e) => logger.info(s"Insert Bus Route Definition for route $busRoute not completed successfully", e)
    }
  }

  def getBusRouteDefinitionsFromDB = BusDefinitionsDBController.loadBusRouteDefinitionsFromDB(dBCollection)


}



