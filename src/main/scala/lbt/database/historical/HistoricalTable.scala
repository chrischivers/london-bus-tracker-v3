package lbt.database.historical

import com.typesafe.scalalogging.StrictLogging
import lbt.DatabaseConfig
import lbt.comon.{BusRoute, BusStop}
import lbt.database._
import lbt.database.definitions.BusDefinitionsTable
import lbt.historical.RecordedVehicleDataToPersist

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class HistoricalTable(dbConfig: DatabaseConfig, busDefinitionsTable: BusDefinitionsTable)(implicit ec: ExecutionContext) extends StrictLogging {

  val historicalDBController = new HistoricalDynamoDBController(dbConfig)(ec)
  var numberToProcess: Long = 0

  def insertHistoricalRecordIntoDB(vehicleRecordedData: RecordedVehicleDataToPersist) = {
    historicalDBController.insertHistoricalRecordIntoDB(vehicleRecordedData)
  }

  def getHistoricalRecordFromDbByBusRoute(busRoute: BusRoute, fromStopID: Option[String] = None, toStopID: Option[String] = None, fromTime: Option[Long] = None, toTime: Option[Long] = None, fromSecOfWeek: Option[Int] = None, toSecOfWeek: Option[Int] = None, vehicleReg: Option[String] = None): List[HistoricalJourneyRecordFromDb] = {
    filterHistoricalJourneyRecordListByTimeAndStops(historicalDBController.loadHistoricalRecordsFromDbByBusRoute(busRoute), fromStopID, toStopID, fromTime, toTime)
      .filter(rec =>
        (fromSecOfWeek.isEmpty || rec.journey.startingSecondOfWeek >= fromSecOfWeek.get) &&
        (toSecOfWeek.isEmpty || rec.journey.startingSecondOfWeek <= toSecOfWeek.get))
      .filter(rec =>
        (vehicleReg.isEmpty || rec.journey.vehicleReg == vehicleReg.get) &&
          rec.stopRecords.nonEmpty)
  }

  def getHistoricalRecordFromDbByVehicle(vehicleReg: String, fromStopID: Option[String] = None, toStopID: Option[String] = None, fromTime: Option[Long] = None, toTime: Option[Long] = None,  fromSecOfWeek: Option[Int] = None, toSecOfWeek: Option[Int] = None, busRoute: Option[BusRoute] = None, limit: Int = 100): List[HistoricalJourneyRecordFromDb] = {
    filterHistoricalJourneyRecordListByTimeAndStops(historicalDBController.loadHistoricalRecordsFromDbByVehicle(vehicleReg, limit), fromStopID, toStopID, fromTime, toTime)
      .filter(rec =>
        (fromSecOfWeek.isEmpty || rec.journey.startingSecondOfWeek >= fromSecOfWeek.get) &&
          (toSecOfWeek.isEmpty || rec.journey.startingSecondOfWeek <= toSecOfWeek.get))
      .filter(rec =>
        (busRoute.isEmpty || rec.journey.busRoute == busRoute.get) &&
          rec.stopRecords.nonEmpty)
  }

  def getHistoricalRecordFromDbByStop(stopID: String, fromTime: Option[Long] = None, toTime: Option[Long] = None, fromSecOfWeek: Option[Int] = None, toSecOfWeek: Option[Int] = None, busRoute: Option[BusRoute] = None, vehicleReg: Option[String] = None, limit: Int = 100): List[HistoricalStopRecordFromDb] = {
    val routesContainingStops = busDefinitionsTable.getBusRouteDefinitions().filter(route => route._2.exists(stop => stop.stopID == stopID)).keys
    val routesContainingStopsWithFilter = busRoute match {
      case Some(thisRoute) => routesContainingStops.filter(route => route == thisRoute)
      case None => routesContainingStops
    }
    routesContainingStopsWithFilter.flatMap(route => getHistoricalRecordFromDbByBusRoute(route, None, None, fromTime, toTime, fromSecOfWeek, toSecOfWeek, vehicleReg)).toList
      .flatMap(rec => rec.stopRecords.find(stop => stop.stopID == stopID)
        .map(arrivalRecord => HistoricalStopRecordFromDb(arrivalRecord.stopID, arrivalRecord.arrivalTime, rec.journey))
      .filter(stopRec =>
        (fromTime.isEmpty || stopRec.arrivalTime >= fromTime.get) &&
          (toTime.isEmpty || stopRec.arrivalTime <= toTime.get))
      .filter(rec =>
        (fromSecOfWeek.isEmpty || rec.journey.startingSecondOfWeek >= fromSecOfWeek.get) &&
          (toSecOfWeek.isEmpty || rec.journey.startingSecondOfWeek <= toSecOfWeek.get))
      .filter(rec =>
        (vehicleReg.isEmpty || rec.journey.vehicleReg == vehicleReg.get) &&
          (busRoute.isEmpty || rec.journey.busRoute == busRoute.get)))
  }

  def deleteTable = historicalDBController.deleteHistoricalTable

  private def filterHistoricalJourneyRecordListByTimeAndStops(historicalJourneyRecordFromDb: List[HistoricalJourneyRecordFromDb], fromStopID: Option[String], toStopID: Option[String], fromTime: Option[Long], toTime: Option[Long]) = {
    historicalJourneyRecordFromDb.map(rec => HistoricalJourneyRecordFromDb(rec.journey, rec.stopRecords
      .filter(stopRec =>
        (fromStopID.isEmpty || rec.stopRecords.indexWhere(x => x.stopID == stopRec.stopID) >= rec.stopRecords.indexWhere(x => x.stopID == fromStopID.get)) &&
          (toStopID.isEmpty || rec.stopRecords.indexWhere(x => x.stopID == stopRec.stopID) <= rec.stopRecords.indexWhere(x => x.stopID == toStopID.get)) &&
          (fromTime.isEmpty || stopRec.arrivalTime >= fromTime.get) &&
          (toTime.isEmpty || stopRec.arrivalTime <= toTime.get))))

  }
}



