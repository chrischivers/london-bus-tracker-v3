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

    val recordsFromDB = historicalTable.getHistoricalRecordFromDbByBusRoute(busRoute, fromJourneyStartMillis, toJourneyStartMillis, fromJourneyStartSecOfWeek, toJourneyStartSecOfWeek, vehicleReg)
    val recordsFromLiveActors = vehicleActorSupervisor.getLiveArrivalRecordsForRoute(busRoute)

    val combinedRecords = for {
      y <- recordsFromLiveActors
      x <- recordsFromDB
    } yield x ++ y

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

    val recordsFromDB = historicalTable.getHistoricalRecordFromDbByVehicle(vehicleReg, fromJourneyStartMillis, toJourneyStartMillis, fromJourneyStartSecOfWeek, toJourneyStartSecOfWeek, busRoute)
    val recordsFromLiveActors = vehicleActorSupervisor.getLiveArrivalRecordsForVehicle(vehicleReg)

    val combinedRecords = for {
      x <- recordsFromLiveActors
      y <- recordsFromDB
    } yield x ++ y
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

    val routesContainingStops = busDefinitionsTable.getBusRouteDefinitions().filter(route => route._2.exists(stop => stop.stopID == stopID)).keys.toList
    val routesContainingStopsWithFilter = busRoute match {
      case Some(thisRoute) => routesContainingStops.filter(route => route == thisRoute)
      case None => routesContainingStops
    }

    val historicalJourneyRecordsFromDB = historicalTable.getHistoricalRecordFromDbByStop(routesContainingStopsWithFilter, stopID, fromArrivalTimeMillis, toArrivalTimeMillis, fromArrivalTimeSecOfWeek, toArrivalTimeSecOfWeek, busRoute, vehicleReg)
    val recordsFromLiveActors = vehicleActorSupervisor.getLiveArrivalRecordsForStop(routesContainingStopsWithFilter, stopID)

    for {
      x <- recordsFromLiveActors
      y <- historicalJourneyRecordsFromDB

    } yield {
      val formattedDBRecords = y.flatMap(rec => rec.stopRecords.find(stop => stop.stopID == stopID)
        .map(arrivalRecord => HistoricalStopRecord(arrivalRecord.stopID, arrivalRecord.arrivalTime, rec.journey, rec.source))
        .filter(stopRec =>
          (fromArrivalTimeMillis.isEmpty || stopRec.arrivalTime >= fromArrivalTimeMillis.get) &&
            (toArrivalTimeMillis.isEmpty || stopRec.arrivalTime <= toArrivalTimeMillis.get)))
      x ++ formattedDBRecords
    }
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



