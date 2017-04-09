package lbt.historical

import java.io

import com.typesafe.scalalogging.StrictLogging
import lbt.DatabaseConfig
import lbt.comon.{BusRoute, BusStop, Commons}
import lbt.database.definitions.BusDefinitionsTable
import lbt.database.historical.{HistoricalJourneyRecord, HistoricalStopRecord, HistoricalTable, Journey}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class HistoricalRecordsFetcher(dbConfig: DatabaseConfig, busDefinitionsTable: BusDefinitionsTable, vehicleActorSupervisor: VehicleActorSupervisor, historicalTable: HistoricalTable)(implicit ec: ExecutionContext) extends StrictLogging {

  def getsHistoricalRecordsByBusRoute
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
  : Future[List[HistoricalJourneyRecord]] = {

    val combinedRecords = for {
      recordsFromDB <- historicalTable.getHistoricalRecordFromDbByBusRoute(busRoute, fromJourneyStartMillis, toJourneyStartMillis, fromJourneyStartSecOfWeek, toJourneyStartSecOfWeek, vehicleReg)
      recordsFromLiveActors <- vehicleActorSupervisor.getLiveArrivalRecordsForRoute(busRoute)
    } yield recordsFromLiveActors ++ recordsFromDB

    filterHistoricalJourneyRecordListByTimeAndStops(combinedRecords, fromStopID, toStopID, fromArrivalTimeMillis, toArrivalTimeMillis, fromArrivalTimeSecOfWeek, toArrivalTimeSecOfWeek)
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
   busRoute: Option[BusRoute] = None)
  : Future[List[HistoricalJourneyRecord]] = {

    val combinedRecords = for {
      recordsFromDB <- historicalTable.getHistoricalRecordFromDbByVehicle(vehicleReg, fromJourneyStartMillis, toJourneyStartMillis, fromJourneyStartSecOfWeek, toJourneyStartSecOfWeek, busRoute)
      recordsFromLiveActors <- vehicleActorSupervisor.getLiveArrivalRecordsForVehicle(vehicleReg)
    } yield recordsFromLiveActors ++ recordsFromDB
    filterHistoricalJourneyRecordListByTimeAndStops(combinedRecords, stopID, stopID, fromArrivalTimeMillis, toArrivalTimeMillis, fromArrivalTimeSecOfWeek, toArrivalTimeSecOfWeek)

  }

  def getHistoricalRecordFromDbByStop
  (stopID: String,
   fromArrivalTimeMillis: Option[Long] = None,
   toArrivalTimeMillis: Option[Long] = None,
   fromArrivalTimeSecOfWeek: Option[Int] = None,
   toArrivalTimeSecOfWeek: Option[Int] = None,
   busRoute: Option[BusRoute] = None,
   vehicleReg: Option[String] = None)
  : Future[List[HistoricalStopRecord]] = {

    for {
      historicalJourneyRecords <- historicalTable.getHistoricalRecordFromDbByStop(stopID, fromArrivalTimeMillis, toArrivalTimeMillis, fromArrivalTimeSecOfWeek, toArrivalTimeSecOfWeek, busRoute, vehicleReg)
      recordsFromDBFormatted = historicalJourneyRecords.flatMap(rec => rec.stopRecords.find(stop => stop.stopID == stopID)
        .map(arrivalRecord => HistoricalStopRecord(arrivalRecord.stopID, arrivalRecord.arrivalTime, rec.journey, rec.source))
        .filter(stopRec =>
          (fromArrivalTimeMillis.isEmpty || stopRec.arrivalTime >= fromArrivalTimeMillis.get) &&
            (toArrivalTimeMillis.isEmpty || stopRec.arrivalTime <= toArrivalTimeMillis.get)))

      recordsFromLiveActors <- vehicleActorSupervisor.getLiveArrivalRecordsForStop(stopID)
    } yield recordsFromLiveActors ++ recordsFromDBFormatted
  }

  private def filterHistoricalJourneyRecordListByTimeAndStops
  (historicalJourneyRecordFromDb: Future[List[HistoricalJourneyRecord]],
   fromStopID: Option[String], toStopID: Option[String],
   fromArrivalTimeMillis: Option[Long],
   toArrivalTimeMillis: Option[Long],
   fromArrivalTimeSecOfWeek: Option[Int],
   toArrivalTimeSecOfWeek: Option[Int])
  : Future[List[HistoricalJourneyRecord]] = {
    for {
      x <- historicalJourneyRecordFromDb
      y = x.map(rec =>
        HistoricalJourneyRecord(
          rec.journey, rec.source,
        rec.stopRecords
      .filter(stopRec =>
      (fromStopID.isEmpty || rec.stopRecords.indexWhere(x => x.stopID == stopRec.stopID) >= rec.stopRecords.indexWhere(x => x.stopID == fromStopID.get)) &&
      (toStopID.isEmpty || rec.stopRecords.indexWhere(x => x.stopID == stopRec.stopID) <= rec.stopRecords.indexWhere(x => x.stopID == toStopID.get)) &&
       (fromArrivalTimeMillis.isEmpty || stopRec.arrivalTime >= fromArrivalTimeMillis.get) &&
       (toArrivalTimeMillis.isEmpty || stopRec.arrivalTime <= toArrivalTimeMillis.get) &&
       (fromArrivalTimeSecOfWeek.isEmpty || Commons.getSecondsOfWeek(stopRec.arrivalTime) >= fromArrivalTimeSecOfWeek.get) &&
       (toArrivalTimeSecOfWeek.isEmpty || Commons.getSecondsOfWeek(stopRec.arrivalTime) <= toArrivalTimeSecOfWeek.get)
      )))
      .filter(_.stopRecords.nonEmpty)
    } yield y
  }
}



