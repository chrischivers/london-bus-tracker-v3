package lbt.database.historical

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model._
import com.github.dwhjames.awswrap.dynamodb.{AttributeValue, DynamoDBSerializer, Schema}
import lbt.DatabaseConfig
import lbt.comon.BusRoute
import lbt.historical.RecordedVehicleDataToPersist
import com.github.dwhjames.awswrap.dynamodb._
import com.typesafe.scalalogging.StrictLogging
import lbt.database.DatabaseControllers
import lbt.database.definitions.DefinitionsDBItem
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import net.liftweb.json._
import org.joda.time.DateTime

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

case class HistoricalDBItem(ROUTE_ID_DIRECTION: String, JOURNEY_ID: String, VEHICLE_REG: String, JOURNEY_START_TIME_MILLIS: Long, JOURNEY_START_SECOND_OF_WEEK: Int, ARRIVAL_RECORD: String)

case class ArrivalRecord(seqNo: Int, stopID: String, arrivalTime: Long)
case class Journey(busRoute: BusRoute, vehicleReg: String, startingTimeMillis: Long, startingSecondOfWeek: Int)
case class HistoricalJourneyRecordFromDb(journey: Journey, stopRecords: List[ArrivalRecord])
case class HistoricalStopRecordFromDb(stopID: String, arrivalTime: Long, journey: Journey)

class HistoricalDynamoDBController(databaseConfig: DatabaseConfig)(implicit val ec: ExecutionContext) extends DatabaseControllers with StrictLogging {

  val credentials = new ProfileCredentialsProvider("default")
  val sdkClient = new AmazonDynamoDBAsyncClient(credentials)
  sdkClient.setRegion(Region.getRegion(Regions.US_WEST_2))
  val client = new AmazonDynamoDBScalaClient(sdkClient)
  val mapper = AmazonDynamoDBScalaMapper(client)
  implicit val formats = DefaultFormats

  object Attributes {
    val routeIDDirection = "ROUTE_ID_DIRECTION" //Hash Key
    val journeyID = "JOURNEY_ID" //Range Key
    val vehicleReg = "VEHICLE_REG"
    val journeyStartTimeMills = "JOURNEY_START_TIME_MILLIS"
    val journeyStartSecondOfWeek= "JOURNEY_START_SECOND_OF_WEEK"
    val arrivalRecord = "ARRIVAL_RECORD"
//    val busStopSeqNo = "BUS_STOP_SEQ_NO"
//    val busStopID = "BUS_STOP_ID"
//    val busStopArrivalTimeMillis = "BUS_STOP_ARRIVAL_TIME_MILLIS"

  }

  implicit object historicalSerializer extends DynamoDBSerializer[HistoricalDBItem] {

    override val tableName = databaseConfig.historicalRecordsTableName

    val vehicleRegSecondaryIndexName = "VehicleRegIndex"
    val journeyStartSecondOfWeekIndexName = "JourneyStartSecondOFWeekIndex"

    override val hashAttributeName = Attributes.routeIDDirection
    override def rangeAttributeName = Some(Attributes.journeyID)
    override def primaryKeyOf(historicalItem: HistoricalDBItem) =
      Map(Attributes.routeIDDirection -> historicalItem.ROUTE_ID_DIRECTION,
        Attributes.journeyID -> historicalItem.JOURNEY_ID)
    override def toAttributeMap(historicalItem: HistoricalDBItem) =
      Map(
        Attributes.routeIDDirection -> historicalItem.ROUTE_ID_DIRECTION,
        Attributes.journeyID -> historicalItem.JOURNEY_ID,
        Attributes.vehicleReg -> historicalItem.VEHICLE_REG,
        Attributes.journeyStartTimeMills -> historicalItem.JOURNEY_START_TIME_MILLIS,
        Attributes.journeyStartSecondOfWeek -> historicalItem.JOURNEY_START_SECOND_OF_WEEK,
        Attributes.arrivalRecord -> historicalItem.ARRIVAL_RECORD
      )
    override def fromAttributeMap(item: collection.mutable.Map[String, AttributeValue]) =
      HistoricalDBItem(
        ROUTE_ID_DIRECTION = item(Attributes.routeIDDirection),
        JOURNEY_ID = item(Attributes.journeyID),
        VEHICLE_REG = item(Attributes.vehicleReg),
        JOURNEY_START_TIME_MILLIS = item(Attributes.journeyStartTimeMills),
        JOURNEY_START_SECOND_OF_WEEK = item(Attributes.journeyStartSecondOfWeek),
        ARRIVAL_RECORD = item(Attributes.arrivalRecord)
      )
  }

  createHistoricalTableIfNotExisting

  def insertHistoricalRecordIntoDB(vehicleRecordedData: RecordedVehicleDataToPersist): Unit = {
    val journeyStartTime = vehicleRecordedData.stopArrivalRecords.head.arrivalTime
    val historicalItemToPersist =
      HistoricalDBItem(
        ROUTE_ID_DIRECTION = write(vehicleRecordedData.busRoute),
        JOURNEY_ID = generateJourneyIdKey(journeyStartTime, vehicleRecordedData.vehicleReg),
        VEHICLE_REG = vehicleRecordedData.vehicleReg,
        JOURNEY_START_TIME_MILLIS = journeyStartTime,
        JOURNEY_START_SECOND_OF_WEEK = getSecondsOfWeek(journeyStartTime),
        ARRIVAL_RECORD = write(vehicleRecordedData.stopArrivalRecords)
      )
    numberInsertsRequested.incrementAndGet()
    mapper.batchDump(Seq(historicalItemToPersist)).onComplete {
      case Success(_) => numberInsertsCompleted.incrementAndGet()
      case Failure(e) => numberInsertsFailed.incrementAndGet()
        logger.error("An error has occurred inserting item into Historical DB :" + historicalItemToPersist, e)
    }
  }

  def loadHistoricalRecordsFromDbByBusRoute(busRoute: BusRoute): List[HistoricalJourneyRecordFromDb] = {
    logger.info(s"Loading historical record from DB for route $busRoute")
    numberGetsRequested.incrementAndGet()
    val mappedResult = for {
      result <- mapper.query[HistoricalDBItem](write(busRoute))
      mappedResult = parseJourneyQueryResult(result)
    } yield mappedResult.toList

    Await.result(mappedResult, 30 seconds)
  }


  def loadHistoricalRecordsFromDbByVehicle(vehicleReg: String, limit: Int): List[HistoricalJourneyRecordFromDb]  = {
    logger.info(s"Loading historical record from DB for vehicle $vehicleReg")
    numberGetsRequested.incrementAndGet()
    val mappedResult = for {
      result <- mapper.query[HistoricalDBItem](historicalSerializer.vehicleRegSecondaryIndexName, Attributes.vehicleReg, vehicleReg, None, true, limit)
      mappedResult = parseJourneyQueryResult(result)
    } yield mappedResult.toList

    Await.result(mappedResult, 30 seconds)
  }


  private def parseJourneyQueryResult(result: Seq[HistoricalDBItem]): Seq[HistoricalJourneyRecordFromDb] = {
      result.map { res =>
        HistoricalJourneyRecordFromDb(
          Journey(parse(res.ROUTE_ID_DIRECTION).extract[BusRoute], res.VEHICLE_REG, res.JOURNEY_START_TIME_MILLIS, res.JOURNEY_START_SECOND_OF_WEEK),
          parse(res.ARRIVAL_RECORD).extract[List[ArrivalRecord]]
            .sortBy(arrRec => arrRec.arrivalTime))
      }.sortBy(arrivalRecord => arrivalRecord.journey.startingTimeMillis)(Ordering[Long].reverse)
  }

  private def generateJourneyIdKey(journeyStartTimeMillis: Long, vehicleID: String) = {
    vehicleID + "-" + journeyStartTimeMillis
  }

  private def getSecondsOfWeek(journeyStartTime: Long): Int = {
    val dateTime = new DateTime(journeyStartTime)
    (dateTime.getDayOfWeek * 86400) + dateTime.getSecondOfDay
  }

  def createHistoricalTableIfNotExisting = {
    if(!sdkClient.listTables().getTableNames.contains(databaseConfig.historicalRecordsTableName)) {
      logger.info("Creating Historical Table...")
      val tableRequest =
        new CreateTableRequest()
          .withTableName(databaseConfig.historicalRecordsTableName)
          .withProvisionedThroughput(
            Schema.provisionedThroughput(5L, 8L))
          .withAttributeDefinitions(
            Schema.stringAttribute(Attributes.routeIDDirection),
            Schema.stringAttribute(Attributes.journeyID),
            Schema.stringAttribute(Attributes.vehicleReg),
            Schema.numberAttribute(Attributes.journeyStartSecondOfWeek))
          .withKeySchema(
            Schema.hashKey(Attributes.routeIDDirection),
            Schema.rangeKey(Attributes.journeyID))
          .withGlobalSecondaryIndexes(
            new GlobalSecondaryIndex()
              .withIndexName(historicalSerializer.vehicleRegSecondaryIndexName)
              .withKeySchema(
                Schema.hashKey(Attributes.vehicleReg),
                Schema.rangeKey(Attributes.journeyID))
              .withProvisionedThroughput(
                Schema.provisionedThroughput(5L, 8L))
              .withProjection(
                new Projection()
                  .withProjectionType(ProjectionType.ALL)
              ))
          .withGlobalSecondaryIndexes(
            new GlobalSecondaryIndex()
              .withIndexName(historicalSerializer.journeyStartSecondOfWeekIndexName)
              .withKeySchema(
                Schema.hashKey(Attributes.journeyStartSecondOfWeek),
                Schema.rangeKey(Attributes.journeyID))
              .withProvisionedThroughput(
                Schema.provisionedThroughput(5L, 8L))
              .withProjection(
                new Projection()
                  .withProjectionType(ProjectionType.ALL)
              ))

      val createTableCommand = Future(sdkClient.createTableAsync(tableRequest).get())
      Await.result(createTableCommand, 20 seconds)
      Thread.sleep(15000)
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

