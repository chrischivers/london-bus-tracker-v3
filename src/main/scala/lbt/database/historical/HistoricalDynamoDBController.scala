package lbt.database.historical

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model.{CreateTableRequest, DeleteTableRequest}
import com.github.dwhjames.awswrap.dynamodb.{AttributeValue, DynamoDBSerializer, Schema}
import lbt.DatabaseConfig
import lbt.comon.BusRoute
import lbt.historical.RecordedVehicleDataToPersist
import com.github.dwhjames.awswrap.dynamodb._
import com.typesafe.scalalogging.StrictLogging
import lbt.database.definitions.DefinitionsDBItem
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import net.liftweb.json._
import org.joda.time.DateTime

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

case class HistoricalDBItem(ROUTE_ID_DIRECTION: String, START_TIME_MILLIS: Long, START_MIN_OF_DAY: Int, START_DAY_OF_WEEK: Int, VEHICLE_REG: String, ARRIVAL_RECORDS: String)
case class ArrivalRecord(seqNo: Int, stopID: String, arrivalTime: Long)
case class HistoricalRecordFromDb(busRoute: BusRoute, vehicleID: String, stopRecords: List[ArrivalRecord])

class HistoricalDynamoDBController(databaseConfig: DatabaseConfig)(implicit val ec: ExecutionContext) extends StrictLogging {

  val credentials = new ProfileCredentialsProvider("default")
  val sdkClient = new AmazonDynamoDBAsyncClient(credentials)
  sdkClient.setRegion(Region.getRegion(Regions.US_WEST_2))
  val client = new AmazonDynamoDBScalaClient(sdkClient)
  val mapper = AmazonDynamoDBScalaMapper(client)
  implicit val formats = DefaultFormats

  object Attributes {
    val route = "ROUTE_ID_DIRECTION"
    val startTimeMills = "START_TIME_MILLIS"
    val startMinOfDay = "START_MIN_OF_DAY"
    val startDayOfWeek = "START_DAY_OF_WEEK"
    val vehicleReg = "VEHICLE_REG"
    val arrivalRecords = "ARRIVAL_RECORDS"
  }

  implicit object historicalSerializer extends DynamoDBSerializer[HistoricalDBItem] {

    override val tableName = databaseConfig.historicalRecordsTableName
    override val hashAttributeName = Attributes.route
    override def rangeAttributeName = Some(Attributes.startTimeMills)
    override def primaryKeyOf(historicalItem: HistoricalDBItem) =
      Map(Attributes.route -> historicalItem.ROUTE_ID_DIRECTION,
        Attributes.startTimeMills -> historicalItem.START_TIME_MILLIS)
    override def toAttributeMap(historicalItem: HistoricalDBItem) =
      Map(
        Attributes.route -> historicalItem.ROUTE_ID_DIRECTION,
        Attributes.startTimeMills -> historicalItem.START_TIME_MILLIS,
        Attributes.startMinOfDay -> historicalItem.START_MIN_OF_DAY,
        Attributes.startDayOfWeek -> historicalItem.START_DAY_OF_WEEK,
        Attributes.vehicleReg -> historicalItem.VEHICLE_REG,
        Attributes.arrivalRecords -> historicalItem.ARRIVAL_RECORDS
      )
    override def fromAttributeMap(item: collection.mutable.Map[String, AttributeValue]) =
      HistoricalDBItem(
        ROUTE_ID_DIRECTION = item(Attributes.route),
        START_TIME_MILLIS = item(Attributes.startTimeMills),
        START_MIN_OF_DAY = item(Attributes.startMinOfDay),
        START_DAY_OF_WEEK = item(Attributes.startDayOfWeek),
        VEHICLE_REG = item(Attributes.vehicleReg),
        ARRIVAL_RECORDS = item(Attributes.arrivalRecords)
      )
  }

  createHistoricalTableIfNotExisting

  def insertHistoricalRecordIntoDB(vehicleRecordedData: RecordedVehicleDataToPersist): Unit = {

    val historicaDBItem = HistoricalDBItem(
      ROUTE_ID_DIRECTION = write(vehicleRecordedData.busRoute),
      START_TIME_MILLIS = vehicleRecordedData.stopArrivalRecords.head.arrivalTime,
      START_MIN_OF_DAY = new DateTime(vehicleRecordedData.stopArrivalRecords.head.arrivalTime).getMinuteOfDay,
      START_DAY_OF_WEEK = new DateTime(vehicleRecordedData.stopArrivalRecords.head.arrivalTime).getDayOfWeek,
      VEHICLE_REG = vehicleRecordedData.vehicleReg,
      ARRIVAL_RECORDS = write(vehicleRecordedData.stopArrivalRecords.map(rec => ArrivalRecord(rec.seqNo, rec.busStopId, rec.arrivalTime))))

    mapper.batchDump(Seq(historicaDBItem))
  }

  def loadHistoricalRecordsFromDbByBusRoute(busRoute: BusRoute): List[HistoricalRecordFromDb] = {

    val mappedResult = for {
      result <- mapper.scan[HistoricalDBItem] (Map("ROUTE_ID_DIRECTION" -> ScanCondition.equalTo(write(busRoute))))
      mappedResult = parseScanResult(result)
    } yield mappedResult.toList

    Await.result(mappedResult, 30 seconds)
  }


  def loadHistoricalRecordsFromDbByVehicle(vehicleReg: String): List[HistoricalRecordFromDb]  = {

    val mappedResult = for {
      result <- mapper.scan[HistoricalDBItem] (Map("VEHICLE_REG" -> ScanCondition.equalTo(vehicleReg)))
      mappedResult = parseScanResult(result)
    } yield mappedResult.toList

    Await.result(mappedResult, 30 seconds)
  }

  def loadHistoricalRecordsFromDbByStop(stopID: String): List[HistoricalRecordFromDb] = {
    val mappedResult = for {
      result <- mapper.scan[HistoricalDBItem] (Map("ARRIVAL_RECORDS" -> ScanCondition.contains(stopID)))
      mappedResult = parseScanResult(result)
    } yield mappedResult.toList

    Await.result(mappedResult, 30 seconds)
  }

  private def parseScanResult(result: Seq[HistoricalDBItem]): Seq[HistoricalRecordFromDb] = {
    result.map(result => HistoricalRecordFromDb(
      parse(result.ROUTE_ID_DIRECTION).extract[BusRoute],
      result.VEHICLE_REG,
      parse(result.ARRIVAL_RECORDS).extract[List[ArrivalRecord]]))
  }

  def createHistoricalTableIfNotExisting = {
    if(!sdkClient.listTables().getTableNames.contains(databaseConfig.historicalRecordsTableName)) {
      logger.info("Creating Historical Table...")
      val tableRequest =
        new CreateTableRequest()
          .withTableName(databaseConfig.historicalRecordsTableName)
          .withProvisionedThroughput(
            Schema.provisionedThroughput(10L, 5L))
          .withAttributeDefinitions(
            Schema.stringAttribute(Attributes.route),
            Schema.numberAttribute(Attributes.startTimeMills))
          .withKeySchema(
            Schema.hashKey(Attributes.route),
            Schema.rangeKey(Attributes.startTimeMills))

      val createTableCommand = Future(sdkClient.createTableAsync(tableRequest).get())
      Await.result(createTableCommand, 20 seconds)
      Thread.sleep(5000)
    } else logger.info("Historical Table already exists. Using existing.")
  }

  def deleteHistoricalTable = {
    if(sdkClient.listTables().getTableNames.contains(databaseConfig.historicalRecordsTableName)) {
      logger.info("Deleting Historical Table...")
      val deleteTableRequest =
        new DeleteTableRequest()
          .withTableName(databaseConfig.historicalRecordsTableName)
      val deleteTableCommand = Future(sdkClient.deleteTableAsync(deleteTableRequest).get())
      Await.result(deleteTableCommand, 20 seconds)
      Thread.sleep(3000)
    } else logger.info("No table exists to delete")
  }
}

