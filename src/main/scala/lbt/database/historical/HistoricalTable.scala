package lbt.database.historical

import com.typesafe.scalalogging.StrictLogging
import lbt.DatabaseConfig
import lbt.comon.{BusRoute, Commons}
import lbt.database.definitions.BusDefinitionsTable
import lbt.historical.{RecordedVehicleDataToPersist, VehicleActorParent, VehicleActorSupervisor}

import scala.concurrent.{ExecutionContext, Future}

class HistoricalTable(dbConfig: DatabaseConfig, busDefinitionsTable: BusDefinitionsTable)(implicit ec: ExecutionContext) extends StrictLogging {

  val historicalDBController = new HistoricalDynamoDBController(dbConfig)(ec)
  var numberToProcess: Long = 0

  def insertHistoricalRecordIntoDB(vehicleRecordedData: RecordedVehicleDataToPersist) = {
    historicalDBController.insertHistoricalRecordIntoDB(vehicleRecordedData)
  }

  def getHistoricalRecordFromDbByBusRoute
  (busRoute: BusRoute,
   fromJourneyStartMillis: Option[Long] = None,
   toJourneyStartMillis: Option[Long] = None,
   fromJourneyStartSecOfWeek: Option[Int] = None,
   toJourneyStartSecOfWeek: Option[Int] = None,
   vehicleReg: Option[String] = None)
  : Future[List[HistoricalJourneyRecordFromDb]] = {

    historicalDBController.loadHistoricalRecordsFromDbByBusRoute(busRoute, fromJourneyStartSecOfWeek, toJourneyStartSecOfWeek, fromJourneyStartMillis, toJourneyStartMillis, vehicleReg)
   }

  def getHistoricalRecordFromDbByVehicle
  (vehicleReg: String,
   fromJourneyStartMillis: Option[Long] = None,
   toJourneyStartMillis: Option[Long] = None,
   fromJourneyStartSecOfWeek: Option[Int] = None,
   toJourneyStartSecOfWeek: Option[Int] = None,
   busRoute: Option[BusRoute] = None)
  : Future[List[HistoricalJourneyRecordFromDb]] = {

    historicalDBController.loadHistoricalRecordsFromDbByVehicle(vehicleReg, busRoute, fromJourneyStartSecOfWeek, toJourneyStartSecOfWeek, fromJourneyStartMillis, toJourneyStartMillis)
  }

  def getHistoricalRecordFromDbByStop
  (stopID: String,
   fromArrivalTimeMillis: Option[Long] = None,
   toArrivalTimeMillis: Option[Long] = None,
   fromArrivalTimeSecOfWeek: Option[Int] = None,
   toArrivalTimeSecOfWeek: Option[Int] = None,
   busRoute: Option[BusRoute] = None,
   vehicleReg: Option[String] = None)
  : Future[List[HistoricalJourneyRecordFromDb]] = {
    val routesContainingStops = busDefinitionsTable.getBusRouteDefinitions().filter(route => route._2.exists(stop => stop.stopID == stopID)).keys
    val routesContainingStopsWithFilter = busRoute match {
      case Some(thisRoute) => routesContainingStops.filter(route => route == thisRoute)
      case None => routesContainingStops
    }
   Future.sequence(routesContainingStopsWithFilter.map(route => getHistoricalRecordFromDbByBusRoute(route, None, None, fromArrivalTimeSecOfWeek, toArrivalTimeSecOfWeek, vehicleReg))).map(_.flatten).map(_.toList)
  }

  def deleteTable = historicalDBController.deleteHistoricalTable

}



