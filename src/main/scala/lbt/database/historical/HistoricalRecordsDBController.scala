package lbt.database.historical

import com.mongodb.casbah.Imports.{DBObject, _}
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.{Imports, MongoDBObject}
import com.typesafe.scalalogging.StrictLogging
import lbt.comon._
import lbt.database.historical.HISTORICAL_RECORDS_DOCUMENT.HISTORICAL_VEHICLE_RECORD_DOCUMENT
import lbt.historical.{RecordedVehicleDataToPersist, StopDataRecordToPersist}
import com.mongodb.casbah.Imports._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class VehicleRecordFromDb(seqNo: Int, stopID: String, arrivalTime: Long)

case class HistoricalRecordFromDb(busRoute: BusRoute, vehicleID: String, stopRecords: List[VehicleRecordFromDb])

object HistoricalRecordsDBController extends StrictLogging {

  def insertHistoricalRecordIntoDB(col: MongoCollection, vehicleRecordedData: RecordedVehicleDataToPersist): Future[Boolean] = {

    Future {
      val newRecord = MongoDBObject(
        HISTORICAL_RECORDS_DOCUMENT.ROUTE_ID -> vehicleRecordedData.busRoute.id,
        HISTORICAL_RECORDS_DOCUMENT.DIRECTION -> vehicleRecordedData.busRoute.direction,
        HISTORICAL_RECORDS_DOCUMENT.STARTING_TIME -> vehicleRecordedData.stopArrivalRecords.head.arrivalTime,
        HISTORICAL_RECORDS_DOCUMENT.VEHICLE_ID -> vehicleRecordedData.vehicleReg,
        HISTORICAL_RECORDS_DOCUMENT.VEHICLE_RECORD ->

          vehicleRecordedData.stopArrivalRecords
            .map {rec: StopDataRecordToPersist =>
              MongoDBObject(
                HISTORICAL_VEHICLE_RECORD_DOCUMENT.SEQ_NO -> rec.seqNo,
                HISTORICAL_VEHICLE_RECORD_DOCUMENT.STOP_ID -> rec.busStopId,
                HISTORICAL_VEHICLE_RECORD_DOCUMENT.ARRIVAL_TIME -> rec.arrivalTime
              )
            }
      )
      val query = MongoDBObject(
        HISTORICAL_RECORDS_DOCUMENT.ROUTE_ID -> vehicleRecordedData.busRoute.id,
        HISTORICAL_RECORDS_DOCUMENT.DIRECTION -> vehicleRecordedData.busRoute.direction,
        HISTORICAL_RECORDS_DOCUMENT.STARTING_TIME -> vehicleRecordedData.stopArrivalRecords.head.arrivalTime,
        HISTORICAL_RECORDS_DOCUMENT.VEHICLE_ID -> vehicleRecordedData.vehicleReg
      )

      col.update(query, newRecord, upsert = true).wasAcknowledged()
    }
  }

  def loadHistoricalRecordsFromDbByBusRoute(col: MongoCollection, busRoute: BusRoute): List[HistoricalRecordFromDb] = {

    val query = MongoDBObject(
      HISTORICAL_RECORDS_DOCUMENT.ROUTE_ID -> busRoute.id,
      HISTORICAL_RECORDS_DOCUMENT.DIRECTION -> busRoute.direction
    )
    executeQuery(col, query)
  }

  def loadHistoricalRecordsFromDbByVehicle(col: MongoCollection, vehicleReg: String): List[HistoricalRecordFromDb] = {

    val query = MongoDBObject(
      HISTORICAL_RECORDS_DOCUMENT.VEHICLE_ID -> vehicleReg
    )
    executeQuery(col, query)
  }

  def loadHistoricalRecordsFromDbByStop(col: MongoCollection, stopID: String): List[HistoricalRecordFromDb] = {
    val query = HISTORICAL_RECORDS_DOCUMENT.VEHICLE_RECORD $elemMatch MongoDBObject(HISTORICAL_VEHICLE_RECORD_DOCUMENT.STOP_ID -> stopID)
    executeQuery(col, query)
  }

  private def executeQuery(col: MongoCollection, query: MongoDBObject): List[HistoricalRecordFromDb] = {
    val cursor = col.find(query)
    cursor.map(route => {
      HistoricalRecordFromDb(
        BusRoute(
          route.getAs[String](HISTORICAL_RECORDS_DOCUMENT.ROUTE_ID).get,
          route.getAs[String](HISTORICAL_RECORDS_DOCUMENT.DIRECTION).get
        ),
        route.getAs[String](HISTORICAL_RECORDS_DOCUMENT.VEHICLE_ID).get,
        route.getAs[List[DBObject]](HISTORICAL_RECORDS_DOCUMENT.VEHICLE_RECORD).get
          .sortBy(record => record.getAs[Int](HISTORICAL_VEHICLE_RECORD_DOCUMENT.SEQ_NO))
          .map(record => {
            VehicleRecordFromDb(record.getAs[Int](HISTORICAL_VEHICLE_RECORD_DOCUMENT.SEQ_NO).get,
              record.getAs[String](HISTORICAL_VEHICLE_RECORD_DOCUMENT.STOP_ID).get,
              record.getAs[Long](HISTORICAL_VEHICLE_RECORD_DOCUMENT.ARRIVAL_TIME).get)
          }
          )
      )
    }
    ) toList
  }
}


case object HISTORICAL_RECORDS_DOCUMENT {
  val ROUTE_ID = "ROUTE_ID"
  val DIRECTION = "DIRECTION"
  val STARTING_TIME = "STARTING_TIME"
  val VEHICLE_ID = "VEHICLE_ID"
  val VEHICLE_RECORD = "VEHICLE_RECORD"

  case object HISTORICAL_VEHICLE_RECORD_DOCUMENT {
    val SEQ_NO = "SEQ_NO"
    val STOP_ID = "STOP_ID"
    val ARRIVAL_TIME = "ARRIVAL_TIME"
  }
}

//TODO look at serialisation of case classes