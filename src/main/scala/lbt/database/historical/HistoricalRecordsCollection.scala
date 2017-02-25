package lbt.database.historical

import com.typesafe.scalalogging.StrictLogging
import lbt.DatabaseConfig
import lbt.comon.BusRoute
import lbt.database._
import lbt.historical.RecordedVehicleDataToPersist

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class HistoricalRecordsCollection(dbConfig: DatabaseConfig)(implicit ec: ExecutionContext) extends DatabaseCollections with StrictLogging{

  override val db: MongoDatabase = new MongoDatabase(dbConfig)
  override val collectionName: String = dbConfig.historicalRecordsCollectionName
  override val indexKeyList = List((HISTORICAL_RECORDS_DOCUMENT.ROUTE_ID, 1), (HISTORICAL_RECORDS_DOCUMENT.DIRECTION, 1), (HISTORICAL_RECORDS_DOCUMENT.STARTING_TIME, 1), (HISTORICAL_RECORDS_DOCUMENT.VEHICLE_ID, 1))
  override val uniqueIndex = true

  var numberToProcess:Long = 0

  def insertHistoricalRecordIntoDB(vehicleRecordedData: RecordedVehicleDataToPersist) = {
    incrementLogRequest(IncrementNumberInsertsRequested(1))
    HistoricalRecordsDBController.insertHistoricalRecordIntoDB(dBCollection, vehicleRecordedData).onComplete {
      case Success(ack) =>  if (ack) incrementLogRequest(IncrementNumberInsertsCompleted(1))
                            else logger.info(s"Insert VehicleRecorded data for route ${vehicleRecordedData.busRoute} and vehicle ${vehicleRecordedData.vehicleID} was not acknowledged by DB")
      case Failure(e) => logger.info(s"Insert Bus Route Definition for route ${vehicleRecordedData.busRoute} and vehicle ${vehicleRecordedData.vehicleID} was not completed successfully", e)
    }
  }

  def getHistoricalRecordFromDB(busRoute: BusRoute):  List[HistoricalRecordFromDb] =
    HistoricalRecordsDBController.loadHistoricalRecordsFromDB(dBCollection, busRoute)

}



