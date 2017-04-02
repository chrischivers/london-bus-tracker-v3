package lbt.database.historical

import com.typesafe.scalalogging.StrictLogging
import lbt.DatabaseConfig
import lbt.comon.{BusRoute, Commons}
import lbt.database.definitions.BusDefinitionsTable
import lbt.historical.RecordedVehicleDataToPersist

import scala.concurrent.ExecutionContext

class HistoricalTable(dbConfig: DatabaseConfig, busDefinitionsTable: BusDefinitionsTable)(implicit ec: ExecutionContext) extends StrictLogging {

  val historicalDBController = new HistoricalDynamoDBController(dbConfig)(ec)
  var numberToProcess: Long = 0

  def insertHistoricalRecordIntoDB(vehicleRecordedData: RecordedVehicleDataToPersist) = {
    historicalDBController.insertHistoricalRecordIntoDB(vehicleRecordedData)
  }

  def getHistoricalRecordFromDbByBusRoute
  (busRoute: BusRoute,
   fromStopID: Option[String] = None,
   toStopID: Option[String] = None,
   fromArrivalTimeMillis: Option[Long] = None,
   toArrivalTimeMillis: Option[Long] = None,
   fromArrivalTimeSecOfWeek: Option[Int] = None,
   toArrivalTimeSecOfWeek: Option[Int] = None,
   fromJourneyStartMillis: Option[Long] = None,
   toJourneyStartMillis: Option[Long] = None,
   fromJourneyStartSecOfWeek: Option[Int] = None,
   toJourneyStartSecOfWeek: Option[Int] = None,
   vehicleReg: Option[String] = None)
  : List[HistoricalJourneyRecordFromDb] = {

    filterHistoricalJourneyRecordListByTimeAndStops(historicalDBController.loadHistoricalRecordsFromDbByBusRoute(busRoute, fromJourneyStartSecOfWeek, toJourneyStartSecOfWeek, fromJourneyStartMillis, toJourneyStartMillis, vehicleReg), fromStopID, toStopID, fromArrivalTimeMillis, toArrivalTimeMillis, fromArrivalTimeSecOfWeek, toArrivalTimeSecOfWeek)
    .filter(_.stopRecords.nonEmpty)
  }

  def getHistoricalRecordFromDbByVehicle
  (vehicleReg: String,
   stopID: Option[String] = None,
   fromArrivalTimeMillis: Option[Long] = None,
   toArrivalTimeMillis: Option[Long] = None,
   fromArrivalTimeSecOfWeek: Option[Int] = None,
   toArrivalTimeSecOfWeek: Option[Int] = None,
   fromJourneyStartMillis: Option[Long] = None,
   toJourneyStartMillis: Option[Long] = None,
   fromJourneyStartSecOfWeek: Option[Int] = None,
   toJourneyStartSecOfWeek: Option[Int] = None,
   busRoute: Option[BusRoute] = None): List[HistoricalJourneyRecordFromDb] = {

    filterHistoricalJourneyRecordListByTimeAndStops(historicalDBController.loadHistoricalRecordsFromDbByVehicle(vehicleReg, stopID, busRoute, fromJourneyStartSecOfWeek, toJourneyStartSecOfWeek, fromJourneyStartMillis, toJourneyStartMillis), stopID, stopID, fromArrivalTimeMillis, toArrivalTimeMillis, fromArrivalTimeSecOfWeek, toArrivalTimeSecOfWeek)
      .filter(_.stopRecords.nonEmpty)
  }

  def getHistoricalRecordFromDbByStop
  (stopID: String,
   fromArrivalTimeMillis: Option[Long] = None,
   toArrivalTimeMillis: Option[Long] = None,
   fromArrivalTimeSecOfWeek: Option[Int] = None,
   toArrivalTimeSecOfWeek: Option[Int] = None,
   busRoute: Option[BusRoute] = None,
   vehicleReg: Option[String] = None): List[HistoricalStopRecordFromDb] = {
    val routesContainingStops = busDefinitionsTable.getBusRouteDefinitions().filter(route => route._2.exists(stop => stop.stopID == stopID)).keys
    val routesContainingStopsWithFilter = busRoute match {
      case Some(thisRoute) => routesContainingStops.filter(route => route == thisRoute)
      case None => routesContainingStops
    }
    routesContainingStopsWithFilter.flatMap(route => getHistoricalRecordFromDbByBusRoute(route, None, None, fromArrivalTimeMillis, toArrivalTimeMillis, fromArrivalTimeSecOfWeek, toArrivalTimeSecOfWeek, None, None, None, None, vehicleReg)).toList
      .flatMap(rec => rec.stopRecords.find(stop => stop.stopID == stopID)
        .map(arrivalRecord => HistoricalStopRecordFromDb(arrivalRecord.stopID, arrivalRecord.arrivalTime, rec.journey))
      .filter(stopRec =>
        (fromArrivalTimeMillis.isEmpty || stopRec.arrivalTime >= fromArrivalTimeMillis.get) &&
          (toArrivalTimeMillis.isEmpty || stopRec.arrivalTime <= toArrivalTimeMillis.get)))
  }

  def deleteTable = historicalDBController.deleteHistoricalTable

  private def filterHistoricalJourneyRecordListByTimeAndStops
  (historicalJourneyRecordFromDb: List[HistoricalJourneyRecordFromDb],
   fromStopID: Option[String], toStopID: Option[String],
   fromArrivalTimeMillis: Option[Long],
   toArrivalTimeMillis: Option[Long],
   fromArrivalTimeSecOfWeek: Option[Int],
   toArrivalTimeSecOfWeek: Option[Int]) = {
    historicalJourneyRecordFromDb.map(rec => HistoricalJourneyRecordFromDb(rec.journey, rec.stopRecords
      .filter(stopRec =>
        (fromStopID.isEmpty || rec.stopRecords.indexWhere(x => x.stopID == stopRec.stopID) >= rec.stopRecords.indexWhere(x => x.stopID == fromStopID.get)) &&
          (toStopID.isEmpty || rec.stopRecords.indexWhere(x => x.stopID == stopRec.stopID) <= rec.stopRecords.indexWhere(x => x.stopID == toStopID.get)) &&
          (fromArrivalTimeMillis.isEmpty || stopRec.arrivalTime >= fromArrivalTimeMillis.get) &&
          (toArrivalTimeMillis.isEmpty || stopRec.arrivalTime <= toArrivalTimeMillis.get) &&
          (fromArrivalTimeSecOfWeek.isEmpty || Commons.getSecondsOfWeek(stopRec.arrivalTime) >= fromArrivalTimeSecOfWeek.get) &&
          (toArrivalTimeSecOfWeek.isEmpty || Commons.getSecondsOfWeek(stopRec.arrivalTime) <= toArrivalTimeSecOfWeek.get)
      )))

  }
}



