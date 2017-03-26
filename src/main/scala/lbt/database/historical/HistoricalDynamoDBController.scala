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
import lbt.database.definitions.DefinitionsDBItem
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import net.liftweb.json._
import org.joda.time.DateTime

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

case class HistoricalDBItem(ROUTE_ID_DIRECTION: String, JOURNEY_SECTION_ID: String, VEHICLE_REG: String, JOURNEY_START_TIME_MILLIS: Long, JOURNEY_START_MIN_OF_DAY: Int, JOURNEY_START_DAY_OF_WEEK: Int, BUS_STOP_SEQ_NO: Int, BUS_STOP_ID: String, BUS_STOP_ARRIVAL_TIME_MILLIS: Long)

case class ArrivalRecord(seqNo: Int, stopID: String, arrivalTime: Long)
case class Journey(busRoute: BusRoute, vehicleReg: String, startingTime: Long)
case class HistoricalJourneyRecordFromDb(journey: Journey, stopRecords: List[ArrivalRecord])
case class HistoricalStopRecordFromDb(stopID: String, arrivalTime: Long, journey: Journey)

class HistoricalDynamoDBController(databaseConfig: DatabaseConfig)(implicit val ec: ExecutionContext) extends StrictLogging {

  val credentials = new ProfileCredentialsProvider("default")
  val sdkClient = new AmazonDynamoDBAsyncClient(credentials)
  sdkClient.setRegion(Region.getRegion(Regions.US_WEST_2))
  val client = new AmazonDynamoDBScalaClient(sdkClient)
  val mapper = AmazonDynamoDBScalaMapper(client)
  implicit val formats = DefaultFormats

  object Attributes {
    val routeIDDirection = "ROUTE_ID_DIRECTION" //Hash Key
    val journeySectionID = "JOURNEY_SECTION_ID" //Range Key
    val vehicleReg = "VEHICLE_REG"
    val journeyStartTimeMills = "JOURNEY_START_TIME_MILLIS"
    val journeyStartMinOfDay = "JOURNEY_START_MIN_OF_DAY"
    val journeyStartDayOfWeek = "JOURNEY_START_DAY_OF_WEEK"
    val busStopSeqNo = "BUS_STOP_SEQ_NO"
    val busStopID = "BUS_STOP_ID"
    val busStopArrivalTimeMillis = "BUS_STOP_ARRIVAL_TIME_MILLIS"

  }

  implicit object historicalSerializer extends DynamoDBSerializer[HistoricalDBItem] {

    override val tableName = databaseConfig.historicalRecordsTableName

    val vehicleRegSecondaryIndexName = "VehicleRegIndex"
    val busStopIDSecondaryIndexName = "BusStopIDIndex"

    override val hashAttributeName = Attributes.routeIDDirection
    override def rangeAttributeName = Some(Attributes.journeySectionID)
    override def primaryKeyOf(historicalItem: HistoricalDBItem) =
      Map(Attributes.routeIDDirection -> historicalItem.ROUTE_ID_DIRECTION,
        Attributes.journeySectionID -> historicalItem.JOURNEY_SECTION_ID)
    override def toAttributeMap(historicalItem: HistoricalDBItem) =
      Map(
        Attributes.routeIDDirection -> historicalItem.ROUTE_ID_DIRECTION,
        Attributes.journeySectionID -> historicalItem.JOURNEY_SECTION_ID,
        Attributes.vehicleReg -> historicalItem.VEHICLE_REG,
        Attributes.journeyStartTimeMills -> historicalItem.JOURNEY_START_TIME_MILLIS,
        Attributes.journeyStartMinOfDay -> historicalItem.JOURNEY_START_MIN_OF_DAY,
        Attributes.journeyStartDayOfWeek -> historicalItem.JOURNEY_START_DAY_OF_WEEK,
        Attributes.busStopSeqNo -> historicalItem.BUS_STOP_SEQ_NO,
        Attributes.busStopID -> historicalItem.BUS_STOP_ID,
        Attributes.busStopArrivalTimeMillis -> historicalItem.BUS_STOP_ARRIVAL_TIME_MILLIS
      )
    override def fromAttributeMap(item: collection.mutable.Map[String, AttributeValue]) =
      HistoricalDBItem(
        ROUTE_ID_DIRECTION = item(Attributes.routeIDDirection),
        JOURNEY_SECTION_ID = item(Attributes.journeySectionID),
        VEHICLE_REG = item(Attributes.vehicleReg),
        JOURNEY_START_TIME_MILLIS = item(Attributes.journeyStartTimeMills),
        JOURNEY_START_DAY_OF_WEEK = item(Attributes.journeyStartDayOfWeek),
        JOURNEY_START_MIN_OF_DAY = item(Attributes.journeyStartMinOfDay),
        BUS_STOP_SEQ_NO = item(Attributes.busStopSeqNo),
        BUS_STOP_ID = item(Attributes.busStopID),
        BUS_STOP_ARRIVAL_TIME_MILLIS = item(Attributes.busStopArrivalTimeMillis)
      )
  }

  createHistoricalTableIfNotExisting

  def insertHistoricalRecordIntoDB(vehicleRecordedData: RecordedVehicleDataToPersist): Unit = {
    val journeyStartTime = vehicleRecordedData.stopArrivalRecords.head.arrivalTime
    val historicalItemsToPersist = vehicleRecordedData.stopArrivalRecords.map(record => {
      HistoricalDBItem(
        ROUTE_ID_DIRECTION = write(vehicleRecordedData.busRoute),
        JOURNEY_SECTION_ID = generateJourneySectionKey(journeyStartTime, vehicleRecordedData.vehicleReg, record.seqNo),
        VEHICLE_REG = vehicleRecordedData.vehicleReg,
        JOURNEY_START_TIME_MILLIS = journeyStartTime,
        JOURNEY_START_DAY_OF_WEEK = new DateTime(journeyStartTime).getDayOfWeek,
        JOURNEY_START_MIN_OF_DAY = new DateTime(journeyStartTime).getMinuteOfDay,
        BUS_STOP_SEQ_NO = record.seqNo,
        BUS_STOP_ID = record.busStopId,
        BUS_STOP_ARRIVAL_TIME_MILLIS = record.arrivalTime
      )
    })
    mapper.batchDump(historicalItemsToPersist).onFailure {
      case e => logger.error("An error has occurred inserting items toDB :" + historicalItemsToPersist, e)
    }
  }

  def loadHistoricalRecordsFromDbByBusRoute(busRoute: BusRoute): List[HistoricalJourneyRecordFromDb] = {
    logger.info(s"Loading historical record from DB for route $busRoute")
    val mappedResult = for {
      result <- mapper.query[HistoricalDBItem](write(busRoute))
      mappedResult = parseJourneyQueryResult(result)
    } yield mappedResult.toList

    Await.result(mappedResult, 30 seconds)
  }


  def loadHistoricalRecordsFromDbByVehicle(vehicleReg: String, limit: Int): List[HistoricalJourneyRecordFromDb]  = {
    logger.info(s"Loading historical record from DB for vehicle $vehicleReg")
    val mappedResult = for {
      result <- mapper.query[HistoricalDBItem](historicalSerializer.vehicleRegSecondaryIndexName, Attributes.vehicleReg, vehicleReg, None, true, limit)
      mappedResult = parseJourneyQueryResult(result)
    } yield mappedResult.toList

    Await.result(mappedResult, 30 seconds)
  }

  def loadHistoricalRecordsFromDbByStopID(stopID: String, limit: Int): List[HistoricalStopRecordFromDb]  = {
    logger.info(s"Loading historical record from DB from stopID $stopID")
    val mappedResult = for {
      result <- mapper.query[HistoricalDBItem](historicalSerializer.busStopIDSecondaryIndexName, Attributes.busStopID, stopID, None, true, limit)
      mappedResult = parseStopQueryResult(result)
    } yield mappedResult.toList

    Await.result(mappedResult, 30 seconds)
  }


  private def parseJourneyQueryResult(result: Seq[HistoricalDBItem]): Seq[HistoricalJourneyRecordFromDb] = {
    val groupedResult = result.groupBy(x => Journey(parse(x.ROUTE_ID_DIRECTION).extract[BusRoute], x.VEHICLE_REG, x.JOURNEY_START_TIME_MILLIS))
      groupedResult.map{
        case(journey, historicalDBItem) =>
          HistoricalJourneyRecordFromDb(
          journey,
          historicalDBItem.sortBy(_.BUS_STOP_SEQ_NO).map(record =>
            ArrivalRecord(record.BUS_STOP_SEQ_NO, record.BUS_STOP_ID, record.BUS_STOP_ARRIVAL_TIME_MILLIS)
          ).toList
        )
      }.toSeq
  }

  private def parseStopQueryResult(result: Seq[HistoricalDBItem]): Seq[HistoricalStopRecordFromDb] = {
    result.map(item =>
        HistoricalStopRecordFromDb(
          item.BUS_STOP_ID,
          item.BUS_STOP_ARRIVAL_TIME_MILLIS,
          Journey(parse(item.ROUTE_ID_DIRECTION).extract[BusRoute], item.VEHICLE_REG, item.JOURNEY_START_TIME_MILLIS)
        ))
  }

  private def generateJourneySectionKey(journeyStartTimeMillis: Long, vehicleID: String, seqNo: Int) = {
    journeyStartTimeMillis + "-" + vehicleID + "-" + "%03d".format(seqNo)
  }

  def createHistoricalTableIfNotExisting = {
    if(!sdkClient.listTables().getTableNames.contains(databaseConfig.historicalRecordsTableName)) {
      logger.info("Creating Historical Table...")
      val tableRequest =
        new CreateTableRequest()
          .withTableName(databaseConfig.historicalRecordsTableName)
          .withProvisionedThroughput(
            Schema.provisionedThroughput(15L, 5L))
          .withAttributeDefinitions(
            Schema.stringAttribute(Attributes.routeIDDirection),
            Schema.stringAttribute(Attributes.journeySectionID),
            Schema.stringAttribute(Attributes.vehicleReg),
            Schema.stringAttribute(Attributes.busStopID))
          .withKeySchema(
            Schema.hashKey(Attributes.routeIDDirection),
            Schema.rangeKey(Attributes.journeySectionID))
          .withGlobalSecondaryIndexes(
            new GlobalSecondaryIndex()
              .withIndexName(historicalSerializer.vehicleRegSecondaryIndexName)
              .withKeySchema(
                Schema.hashKey(Attributes.vehicleReg),
                Schema.rangeKey(Attributes.journeySectionID))
              .withProvisionedThroughput(
                Schema.provisionedThroughput(10L, 5L))
              .withProjection(
                new Projection()
                  .withProjectionType(ProjectionType.ALL)
              ),
            new GlobalSecondaryIndex()
              .withIndexName(historicalSerializer.busStopIDSecondaryIndexName)
              .withKeySchema(
                Schema.hashKey(Attributes.busStopID),
                Schema.rangeKey(Attributes.journeySectionID))
              .withProvisionedThroughput(
                Schema.provisionedThroughput(10L, 5L))
              .withProjection(
                new Projection()
                  .withProjectionType(ProjectionType.ALL)
              )
          )

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

