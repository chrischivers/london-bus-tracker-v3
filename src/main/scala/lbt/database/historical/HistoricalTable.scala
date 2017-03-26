package lbt.database.historical

import com.typesafe.scalalogging.StrictLogging
import lbt.DatabaseConfig
import lbt.comon.{BusRoute, BusStop}
import lbt.database._
import lbt.database.definitions.BusDefinitionsTable
import lbt.historical.RecordedVehicleDataToPersist

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class HistoricalTable(dbConfig: DatabaseConfig, busDefinitionsTable: BusDefinitionsTable)(implicit ec: ExecutionContext) extends DatabaseTables with StrictLogging {

  private val historicalDBController = new HistoricalDynamoDBController(dbConfig)(ec)
  var numberToProcess: Long = 0

  def insertHistoricalRecordIntoDB(vehicleRecordedData: RecordedVehicleDataToPersist) = {
   numberInsertsRequested.incrementAndGet()
    historicalDBController.insertHistoricalRecordIntoDB(vehicleRecordedData)
    numberInsertsCompleted.incrementAndGet()
  }

  def getHistoricalRecordFromDbByBusRoute(busRoute: BusRoute, fromStopID: Option[String] = None, toStopID: Option[String] = None, fromTime: Option[Long] = None, toTime: Option[Long] = None, vehicleReg: Option[String] = None): List[HistoricalJourneyRecordFromDb] = {
    numberGetsRequested.incrementAndGet()
    historicalDBController.loadHistoricalRecordsFromDbByBusRoute(busRoute)
      .map(rec => HistoricalJourneyRecordFromDb(rec.journey, rec.stopRecords
          .filter(stopRec =>
            (fromStopID.isEmpty || rec.stopRecords.indexWhere(x => x.stopID == stopRec.stopID) >= rec.stopRecords.indexWhere(x => x.stopID == fromStopID.get)) &&
              (toStopID.isEmpty || rec.stopRecords.indexWhere(x => x.stopID == stopRec.stopID) <= rec.stopRecords.indexWhere(x => x.stopID == toStopID.get)) &&
              (fromTime.isEmpty || stopRec.arrivalTime >= fromTime.get) &&
              (toTime.isEmpty || stopRec.arrivalTime <= toTime.get))))
      .filter(rec =>
        (vehicleReg.isEmpty || rec.journey.vehicleReg == vehicleReg.get) &&
          rec.stopRecords.nonEmpty)
  }

  def getHistoricalRecordFromDbByVehicle(vehicleReg: String, fromStopID: Option[String] = None, toStopID: Option[String] = None, fromTime: Option[Long] = None, toTime: Option[Long] = None, busRoute: Option[BusRoute] = None, limit: Int = 100): List[HistoricalJourneyRecordFromDb] = {
    numberGetsRequested.incrementAndGet()
    historicalDBController.loadHistoricalRecordsFromDbByVehicle(vehicleReg, limit)
      .map(rec => HistoricalJourneyRecordFromDb(rec.journey, rec.stopRecords
        .filter(stopRec =>
          (fromStopID.isEmpty || rec.stopRecords.indexWhere(x => x.stopID == stopRec.stopID) >= rec.stopRecords.indexWhere(x => x.stopID == fromStopID.get)) &&
            (toStopID.isEmpty || rec.stopRecords.indexWhere(x => x.stopID == stopRec.stopID) <= rec.stopRecords.indexWhere(x => x.stopID == toStopID.get)) &&
            (fromTime.isEmpty || stopRec.arrivalTime >= fromTime.get) &&
            (toTime.isEmpty || stopRec.arrivalTime <= toTime.get))))
      .filter(rec =>
        (busRoute.isEmpty || rec.journey.busRoute == busRoute.get) &&
          rec.stopRecords.nonEmpty)
  }

  def getHistoricalRecordFromDbByStop(stopID: String, fromTime: Option[Long] = None, toTime: Option[Long] = None, busRoute: Option[BusRoute] = None, vehicleReg: Option[String] = None, limit: Int = 100): List[HistoricalStopRecordFromDb] = {
    numberGetsRequested.incrementAndGet()
    historicalDBController.loadHistoricalRecordsFromDbByStopID(stopID, limit)
      .filter(stopRec =>
          (fromTime.isEmpty || stopRec.arrivalTime >= fromTime.get) &&
          (toTime.isEmpty || stopRec.arrivalTime <= toTime.get))
      .filter(rec =>
        (vehicleReg.isEmpty || rec.journey.vehicleReg == vehicleReg.get) &&
        (busRoute.isEmpty || rec.journey.busRoute == busRoute.get))
  }

  def deleteTable = historicalDBController.deleteHistoricalTable
}



