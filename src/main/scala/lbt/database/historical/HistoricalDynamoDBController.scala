package lbt.database.historical

import java.util

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.model._
import com.github.dwhjames.awswrap.dynamodb.{AttributeValue, DynamoDBSerializer, Schema, _}
import com.typesafe.scalalogging.StrictLogging
import lbt.DatabaseConfig
import lbt.comon.{BusRoute, Commons}
import lbt.database.DatabaseControllers
import lbt.database.definitions.DefinitionsDBItem
import lbt.historical.RecordedVehicleDataToPersist
import net.liftweb.json.Serialization.write
import net.liftweb.json.{DefaultFormats, _}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

case class HistoricalDBItem(ROUTE_ID_DIRECTION: String, JOURNEY_ID: String, VEHICLE_REG: String, JOURNEY_START_TIME_MILLIS: Long, JOURNEY_START_SECOND_OF_WEEK: Int, ARRIVAL_RECORD: String)
case class Source(value: String)
case class ArrivalRecord(seqNo: Int, stopID: String, arrivalTime: Long)
case class Journey(busRoute: BusRoute, vehicleReg: String, startingTimeMillis: Long, startingSecondOfWeek: Int)
case class HistoricalJourneyRecord(journey: Journey, source: Source, stopRecords: List[ArrivalRecord])
case class HistoricalStopRecord(stopID: String, arrivalTime: Long, journey: Journey, source: Source)


class HistoricalDynamoDBController(databaseConfig: DatabaseConfig)(implicit val ec: ExecutionContext) extends DatabaseControllers with StrictLogging {

  val credentials = new ProfileCredentialsProvider("lbt")
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
  }

  implicit object historicalSerializer extends DynamoDBSerializer[HistoricalDBItem] {

    override val tableName = databaseConfig.historicalRecordsTableName

    val vehicleRegSecondaryIndexName = "VehicleRegIndex"
//    val journeyStartSecondOfWeekIndexName = "JourneyStartSecondOFWeekIndex"

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
        JOURNEY_START_SECOND_OF_WEEK = Commons.getSecondsOfWeek(journeyStartTime),
        ARRIVAL_RECORD = write(vehicleRecordedData.stopArrivalRecords)
      )
    numberInsertsRequested.incrementAndGet()
    mapper.batchDump(Seq(historicalItemToPersist)).onComplete {
      case Success(_) => numberInsertsCompleted.incrementAndGet()
      case Failure(e) => numberInsertsFailed.incrementAndGet()
        logger.error("An error has occurred inserting item into Historical DB :" + historicalItemToPersist, e)
    }
  }


  def loadHistoricalRecordsFromDbByBusRoute(busRoute: BusRoute, fromJourneyStartSecOfWeek: Option[Int], toJourneyStartSecOfWeek: Option[Int], fromJourneyStartMillis: Option[Long], toJourneyStartMillis: Option[Long], vehicleReg: Option[String], limit: Int): Future[List[HistoricalJourneyRecord]] = {
    logger.info(s"Loading historical record from DB for route $busRoute")
    numberGetsRequested.incrementAndGet()

    val queryRequest = new QueryRequest()
    queryRequest.withTableName(databaseConfig.historicalRecordsTableName)
    queryRequest.withKeyConditions(Map(Attributes.routeIDDirection -> new Condition()
      .withComparisonOperator(ComparisonOperator.EQ)
      .withAttributeValueList(new AttributeValue().withS(write(busRoute)))).asJava)
      .setScanIndexForward(false)

    val filteredQueryRequest1 = addStartSecOfWeekFilter(queryRequest, fromJourneyStartSecOfWeek, toJourneyStartSecOfWeek)
    val filteredQueryRequest2 = addStartMillisFilter(filteredQueryRequest1,fromJourneyStartMillis, toJourneyStartMillis)
    val filteredQueryRequest3 = addVehicleRegFilter(filteredQueryRequest2, vehicleReg)

    for {
      result <- mapper.query[HistoricalDBItem](filteredQueryRequest3, limit)
      mappedResult = parseDBJourneyQueryResult(result)
    } yield mappedResult.toList
  }


  def loadHistoricalRecordsFromDbByVehicle(vehicleReg: String, busRoute: Option[BusRoute] = None, fromJourneyStartSecOfWeek: Option[Int], toJourneyStartSecOfWeek: Option[Int], fromJourneyStartMillis: Option[Long], toJourneyStartMillis: Option[Long], limit: Int): Future[List[HistoricalJourneyRecord]]  = {
    logger.info(s"Loading historical record from DB for vehicle $vehicleReg")
    numberGetsRequested.incrementAndGet()

      val queryRequest = new QueryRequest()
      queryRequest.withIndexName(historicalSerializer.vehicleRegSecondaryIndexName)
      queryRequest.withKeyConditions(Map(Attributes.vehicleReg -> new Condition()
        .withComparisonOperator(ComparisonOperator.EQ)
        .withAttributeValueList(new AttributeValue().withS(vehicleReg))).asJava)
        .setScanIndexForward(false)

    val filteredQueryRequest1 = addStartSecOfWeekFilter(queryRequest, fromJourneyStartSecOfWeek, toJourneyStartSecOfWeek)
    val filteredQueryRequest2 = addStartMillisFilter(filteredQueryRequest1,fromJourneyStartMillis, toJourneyStartMillis)
    val filteredQueryRequest3 = addBusRouteFilter(filteredQueryRequest2, busRoute)

    for {
      result <- mapper.query[HistoricalDBItem](filteredQueryRequest3, limit)
      mappedResult = parseDBJourneyQueryResult(result)
    } yield mappedResult.toList

  }

  private def parseDBJourneyQueryResult(result: Seq[HistoricalDBItem]): Seq[HistoricalJourneyRecord] = {
      result.map { res =>
        HistoricalJourneyRecord(
          Journey(parse(res.ROUTE_ID_DIRECTION).extract[BusRoute], res.VEHICLE_REG, res.JOURNEY_START_TIME_MILLIS, res.JOURNEY_START_SECOND_OF_WEEK),
          Source("DB"),
          parse(res.ARRIVAL_RECORD).extract[List[ArrivalRecord]]
            .sortBy(arrRec => arrRec.arrivalTime))
      }.sortBy(arrivalRecord => arrivalRecord.journey.startingTimeMillis)(Ordering[Long].reverse)
  }

    private def addStartSecOfWeekFilter(queryRequest: QueryRequest, fromJourneyStartSecOfWeek: Option[Int], toJourneyStartSecOfWeek: Option[Int]):QueryRequest = {
      if (fromJourneyStartSecOfWeek.isDefined || toJourneyStartSecOfWeek.isDefined) {
        val currentFilterExpression = Option(queryRequest.getFilterExpression).map(_ + " and ").getOrElse("")
        queryRequest.withFilterExpression(currentFilterExpression + s"${Attributes.journeyStartSecondOfWeek} between :startSec and :endSec")
        val currentAttributeValuesMap = Option(queryRequest.getExpressionAttributeValues).getOrElse(new util.HashMap[String, AttributeValue]())
        currentAttributeValuesMap.put(":startSec", new AttributeValue().withN(fromJourneyStartSecOfWeek.map(_.toString).getOrElse("0")))
        currentAttributeValuesMap.put(":endSec", new AttributeValue().withN(toJourneyStartSecOfWeek.map(_.toString).getOrElse("604800")))
        queryRequest.withExpressionAttributeValues(currentAttributeValuesMap)
      } else queryRequest
    }

  private def addStartMillisFilter(queryRequest: QueryRequest, fromJourneyStartMillis: Option[Long], toJourneyStartMillis: Option[Long]):QueryRequest  = {
    if (fromJourneyStartMillis.isDefined || toJourneyStartMillis.isDefined) {
      val currentFilterExpression = Option(queryRequest.getFilterExpression).map(_ + " and ").getOrElse("")
      queryRequest.withFilterExpression(currentFilterExpression + s"${Attributes.journeyStartTimeMills} between :startMillis and :toMillis")
      val currentAttributeValuesMap = Option(queryRequest.getExpressionAttributeValues).getOrElse(new util.HashMap[String, AttributeValue]())
      currentAttributeValuesMap.put(":startMillis", new AttributeValue().withN(fromJourneyStartMillis.map(_.toString).getOrElse("0")))
      currentAttributeValuesMap.put(":toMillis", new AttributeValue().withN(toJourneyStartMillis.map(_.toString).getOrElse(System.currentTimeMillis().toString)))
      queryRequest.withExpressionAttributeValues(currentAttributeValuesMap)
    } else queryRequest
  }

  private def addVehicleRegFilter(queryRequest: QueryRequest, vehicleReg: Option[String]):QueryRequest  = {
    if (vehicleReg.isDefined) {
      val currentFilterExpression = Option(queryRequest.getFilterExpression).map(_ + " and ").getOrElse("")
      queryRequest.withFilterExpression(currentFilterExpression + s"${Attributes.vehicleReg} = :vehicleReg")
      val currentAttributeValuesMap = Option(queryRequest.getExpressionAttributeValues).getOrElse(new util.HashMap[String, AttributeValue]())
      currentAttributeValuesMap.put(":vehicleReg", new AttributeValue().withS(vehicleReg.get))
      queryRequest.withExpressionAttributeValues(currentAttributeValuesMap)
    } else queryRequest
  }

  private def addBusRouteFilter(queryRequest: QueryRequest, busRoute: Option[BusRoute]):QueryRequest  = {
    if (busRoute.isDefined) {
      val currentFilterExpression = Option(queryRequest.getFilterExpression).map(_ + " and ").getOrElse("")
      queryRequest.withFilterExpression(currentFilterExpression + s"${Attributes.routeIDDirection} = :busRoute")
      val currentAttributeValuesMap = Option(queryRequest.getExpressionAttributeValues).getOrElse(new util.HashMap[String, AttributeValue]())
      currentAttributeValuesMap.put(":busRoute", new AttributeValue().withS(write(busRoute.get)))
      queryRequest.withExpressionAttributeValues(currentAttributeValuesMap)
    } else queryRequest
  }

  private def generateJourneyIdKey(journeyStartTimeMillis: Long, vehicleID: String) = {
    vehicleID + "-" + journeyStartTimeMillis
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
            Schema.stringAttribute(Attributes.vehicleReg)
               )
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
