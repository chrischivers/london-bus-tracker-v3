package lbt.database.definitions

import akka.actor.{ActorRef, ActorSystem, Props}
import com.mongodb.casbah.Imports.{DBObject, _}
import com.mongodb.casbah.MongoCollection
import lbt.comon.{BusRoute, BusStop, Commons}
import lbt.dataSource.definitions.BusDefinitionsOps.BusRouteDefinitions
import lbt.database._
import lbt.{ConfigLoader, DatabaseConfig, DefinitionsConfig}

class BusDefinitionsCollection(val defConfig: DefinitionsConfig, val dbConfig: DatabaseConfig) extends DatabaseCollections {

  override implicit val system = ActorSystem(dbConfig.databaseActorSystemName)
  override val db: MongoDatabase = new MongoDatabase(dbConfig)
  override val collectionName: String = defConfig.dBCollectionName
  override val fieldsVector = Vector(BUS_ROUTE_DEFINITION_DOCUMENT.ROUTE_ID, BUS_ROUTE_DEFINITION_DOCUMENT.DIRECTION, BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE)
  override val indexKeyList = List((BUS_ROUTE_DEFINITION_DOCUMENT.ROUTE_ID, 1), (BUS_ROUTE_DEFINITION_DOCUMENT.DIRECTION, 1))
  override val uniqueIndex = true
  override val supervisor: ActorRef = system.actorOf(Props(new BusDefinitionsDBSupervisor(this)) , name = "BusDefinitionsDBSupervisor")

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
        Commons.directionStrToDirection(routeDef.getAs[String](BUS_ROUTE_DEFINITION_DOCUMENT.DIRECTION).get)) ->
        routeDef.getAs[List[DBObject]](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE).get
          .sortBy(stopDef => stopDef.getAs[Int](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION.SEQUENCE_NO))
          .map(stopDef => {
            BusStop(
              stopDef.getAs[String](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION.BUS_STOP_ID).get,
              stopDef.getAs[String](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION.BUS_STOP_NAME).get,
              stopDef.getAs[String](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION.LATITUDE).get,
              stopDef.getAs[String](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION.LONGITUDE).get
            )
          })
    }) toMap
  }
}



