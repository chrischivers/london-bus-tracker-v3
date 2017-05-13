package lbt.database.historical

import com.typesafe.scalalogging.StrictLogging
import lbt.{DatabaseConfig, HistoricalRecordsConfig}
import lbt.comon.{BusRoute, Commons}
import lbt.database.definitions.BusDefinitionsTable
import lbt.historical.{RecordedVehicleDataToPersist, VehicleActorParent, VehicleActorSupervisor}

import scala.concurrent.{ExecutionContext, Future}

class HistoricalTable(dbConfig: DatabaseConfig, historicalRecordsConfig: HistoricalRecordsConfig, busDefinitionsTable: BusDefinitionsTable)(implicit ec: ExecutionContext) extends StrictLogging {

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
  : Future[List[HistoricalJourneyRecord]] = {

    historicalDBController.loadHistoricalRecordsFromDbByBusRoute(busRoute, fromJourneyStartSecOfWeek, toJourneyStartSecOfWeek, fromJourneyStartMillis, toJourneyStartMillis, vehicleReg, historicalRecordsConfig.defaultRetrievalLimit)
   }

  def getHistoricalRecordFromDbByVehicle
  (vehicleReg: String,
   fromJourneyStartMillis: Option[Long] = None,
   toJourneyStartMillis: Option[Long] = None,
   fromJourneyStartSecOfWeek: Option[Int] = None,
   toJourneyStartSecOfWeek: Option[Int] = None,
   busRoute: Option[BusRoute] = None)
  : Future[List[HistoricalJourneyRecord]] = {

    historicalDBController.loadHistoricalRecordsFromDbByVehicle(vehicleReg, busRoute, fromJourneyStartSecOfWeek, toJourneyStartSecOfWeek, fromJourneyStartMillis, toJourneyStartMillis, historicalRecordsConfig.defaultRetrievalLimit)
  }

  def getHistoricalRecordFromDbByStop
  (filteredRoutesContainingStop: List[BusRoute],
   stopID: String,
   fromArrivalTimeMillis: Option[Long] = None,
   toArrivalTimeMillis: Option[Long] = None,
   fromArrivalTimeSecOfWeek: Option[Int] = None,
   toArrivalTimeSecOfWeek: Option[Int] = None,
   busRoute: Option[BusRoute] = None,
   vehicleReg: Option[String] = None)
  : Future[List[HistoricalJourneyRecord]] = {

   Future.sequence(filteredRoutesContainingStop.map(route => getHistoricalRecordFromDbByBusRoute(route, None, None, fromArrivalTimeSecOfWeek, toArrivalTimeSecOfWeek, vehicleReg))).map(_.flatten).map(_.toList)
  }

  def deleteTable = historicalDBController.deleteHistoricalTable

  def createTableIfNotExisting = historicalDBController.createHistoricalTableIfNotExisting

}



