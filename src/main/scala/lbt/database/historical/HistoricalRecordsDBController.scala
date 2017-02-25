package lbt.database.historical

import com.mongodb.casbah.Imports.{DBObject, _}
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.{Imports, MongoDBObject}
import com.typesafe.scalalogging.StrictLogging
import lbt.comon.{BusRoute, BusStop, Commons}
import lbt.database.historical.HISTORICAL_RECORDS_DOCUMENT.HISTORICAL_VEHICLE_RECORD_DOCUMENT
import lbt.historical.RecordedVehicleDataToPersist

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class VehicleRecordFromDb(seqNo: Int, stopID: String, arrivalTime: Long)

case class HistoricalRecordFromDb(busRoute: BusRoute, vehicleID: String, stopRecords: List[VehicleRecordFromDb])

object HistoricalRecordsDBController extends StrictLogging {

  def insertHistoricalRecordIntoDB(col: MongoCollection, vehicleRecordedData: RecordedVehicleDataToPersist): Future[Boolean] = {

    Future {
      val newRecord = MongoDBObject(
        HISTORICAL_RECORDS_DOCUMENT.ROUTE_ID -> vehicleRecordedData.busRoute.id,
        HISTORICAL_RECORDS_DOCUMENT.DIRECTION -> vehicleRecordedData.busRoute.direction.toString,
        HISTORICAL_RECORDS_DOCUMENT.STARTING_TIME -> vehicleRecordedData.stopArrivalRecords.head._3,
        HISTORICAL_RECORDS_DOCUMENT.VEHICLE_ID -> vehicleRecordedData.vehicleID,
        HISTORICAL_RECORDS_DOCUMENT.VEHICLE_RECORD ->

          vehicleRecordedData.stopArrivalRecords
            .map {case(seqNo, busStop, arrivalTime) =>
              MongoDBObject(
                HISTORICAL_VEHICLE_RECORD_DOCUMENT.SEQ_NO -> seqNo,
                HISTORICAL_VEHICLE_RECORD_DOCUMENT.STOP_ID -> busStop.id,
                HISTORICAL_VEHICLE_RECORD_DOCUMENT.ARRIVAL_TIME -> arrivalTime
              )
            }
      )
      val query = MongoDBObject(
        HISTORICAL_RECORDS_DOCUMENT.ROUTE_ID -> vehicleRecordedData.busRoute.id,
        HISTORICAL_RECORDS_DOCUMENT.DIRECTION -> vehicleRecordedData.busRoute.direction.toString,
        HISTORICAL_RECORDS_DOCUMENT.STARTING_TIME -> vehicleRecordedData.stopArrivalRecords.head._3,
        HISTORICAL_RECORDS_DOCUMENT.VEHICLE_ID -> vehicleRecordedData.vehicleID
      )

      col.update(query, newRecord, upsert = true).wasAcknowledged()
    }
  }

  def loadHistoricalRecordsFromDB(col: MongoCollection, busRoute: BusRoute): List[HistoricalRecordFromDb] = {

    val query = MongoDBObject(
      HISTORICAL_RECORDS_DOCUMENT.ROUTE_ID -> busRoute.id,
      HISTORICAL_RECORDS_DOCUMENT.DIRECTION -> busRoute.direction.toString
    //  HISTORICAL_RECORDS_DOCUMENT.STARTING_TIME -> vehicleRecordedData.stopArrivalRecords.head._3,
     // HISTORICAL_RECORDS_DOCUMENT.VEHICLE_ID -> vehicleRecordedData.vehicleID
    )
    val cursor = col.find(query)
    cursor.map(route => {
      HistoricalRecordFromDb(
          BusRoute(
            route.getAs[String](HISTORICAL_RECORDS_DOCUMENT.ROUTE_ID).get,
            Commons.toDirection(route.getAs[String](HISTORICAL_RECORDS_DOCUMENT.DIRECTION).get)
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
  /*
   cursor.map(routeDef => {
        BusRoute(
          routeDef.getAs[String](BUS_ROUTE_DEFINITION_DOCUMENT.ROUTE_ID).get,
          Commons.toDirection(routeDef.getAs[String](BUS_ROUTE_DEFINITION_DOCUMENT.DIRECTION).get)) ->
          routeDef.getAs[List[DBObject]](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE).get
            .sortBy(stopDef => stopDef.getAs[Int](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION.SEQUENCE_NO))
            .map(stopDef => {
              BusStop(
                stopDef.getAs[String](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION.BUS_STOP_ID).get,
                stopDef.getAs[String](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION.BUS_STOP_NAME).get,
                stopDef.getAs[Double](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION.LATITUDE).get,
                stopDef.getAs[Double](BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE_DEFINITION.LONGITUDE).get
              )
            })
      }) toMap
   */
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