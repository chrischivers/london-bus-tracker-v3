import lbt.comon.{BusRoute, BusStop, Outbound}
import lbt.{ConfigLoader, MessageConsumer, MessageProcessor}
import lbt.dataSource.Stream.{DataStreamProcessingController, SourceLine, Start, Stop}
import lbt.dataSource.definitions.BusDefinitionsOps
import lbt.database.DbCollections
import lbt.historical.{HistoricalMessageProcessor, HistoricalRecorder}
import net.liftweb.json.{DefaultFormats, parse}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually.scaled
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.concurrent.Eventually._

import scala.util.Random

class HistoricalRecorderTest extends FunSuite with BeforeAndAfter {

  val testMessagingConfig = ConfigLoader.defaultConfig.messagingConfig.copy(
    exchangeName = "test-lbt-exchange",
    historicalRecorderQueueName = "test-historical-recorder-queue-name",
    historicalRecorderRoutingKey = "test-historical-recorder-routing-key")

  val testDataSourceConfig = ConfigLoader.defaultConfig.dataSourceConfig

  val testDBConfig = ConfigLoader.defaultConfig.databaseConfig.copy(databaseName = "TestDB")
  val dbCol = DbCollections.loadBusDefinitionsCollection()

  before {dbCol.db.dropDatabase}

  after {dbCol.db.dropDatabase}

  test("Line should be accepted if route is in definitions or disregarded if not in definitions") {
    implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(60, Seconds)), interval = scaled(Span(500, Millis)))
    val messageProcessor = new HistoricalMessageProcessor()
    val consumer = HistoricalRecorder(messageProcessor, testMessagingConfig)

    val testBusRoute = BusRoute("3", Outbound())
    val getOnlyList = List(testBusRoute)
    BusDefinitionsOps.loadBusDefinitionsOps(dbCol).refreshBusRouteDefinitionFromWeb(getOnly = Some(getOnlyList))
    Thread.sleep(5000)
    val routeDefFromDb = BusDefinitionsOps.loadBusDefinitionsOps(dbCol).busRouteDefinitions(testBusRoute)
    val randomStop = routeDefFromDb(Random.nextInt(routeDefFromDb.size - 1))

    val validSourceline = "[1,\"" + randomStop.id + "\",\"" + testBusRoute.id + "\",1,\"Any Place\",\"SampleReg\"," + System.currentTimeMillis() + "]"
    val invalidSourceLine = "[1,\"490009774E\",\"99XXXX\",2,\"Bromley North\",\"YX62DYN\"," + System.currentTimeMillis() + "]"
    val testLines = List(validSourceline,invalidSourceLine)

    val testDataSource = new TestDataSource(testDataSourceConfig, Some(testLines))
    val dataStreamProcessingController = DataStreamProcessingController(testDataSource, testMessagingConfig)

    dataStreamProcessingController ! Start
    Thread.sleep(500)
    dataStreamProcessingController ! Stop

    eventually {
      testDataSource.numberLinesStreamed shouldBe 2
      consumer.messagesReceived shouldBe 2
      messageProcessor.messagesProcessed shouldBe 2
      testLines should contain (sourceLineBackToLine(messageProcessor.lastProcessedMessage.get))
      validSourceline shouldEqual sourceLineBackToLine(messageProcessor.lastValidatedMessage.get)
      messageProcessor.messagesValidated shouldBe 1
    }
  }

  test("Line should be accepted if bus stop is in definitions and rejected if not") {
    //TODO
    implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(60, Seconds)), interval = scaled(Span(500, Millis)))
    val messageProcessor = new HistoricalMessageProcessor()
    val consumer = HistoricalRecorder(messageProcessor, testMessagingConfig)

    val testBusRoute = BusRoute("3", Outbound())
    val getOnlyList = List(testBusRoute)
    BusDefinitionsOps.loadBusDefinitionsOps(dbCol).refreshBusRouteDefinitionFromWeb(getOnly = Some(getOnlyList))
    Thread.sleep(5000)
    val routeDefFromDb = BusDefinitionsOps.loadBusDefinitionsOps(dbCol).busRouteDefinitions(testBusRoute)
    val randomStop = routeDefFromDb(Random.nextInt(routeDefFromDb.size - 1))

    val validSourceline = "[1,\"" + randomStop.id + "\",\"" + testBusRoute.id + "\",1,\"Any Place\",\"SampleReg\"," + System.currentTimeMillis() + "]"
    val invalidSourceLine = "[1,\"490009774E\",\"99XXXX\",2,\"Bromley North\",\"YX62DYN\"," + System.currentTimeMillis() + "]"
    val testLines = List(validSourceline,invalidSourceLine)

    val testDataSource = new TestDataSource(testDataSourceConfig, Some(testLines))
    val dataStreamProcessingController = DataStreamProcessingController(testDataSource, testMessagingConfig)

    dataStreamProcessingController ! Start
    Thread.sleep(500)
    dataStreamProcessingController ! Stop

    eventually {
      testDataSource.numberLinesStreamed shouldBe 2
      consumer.messagesReceived shouldBe 2
      messageProcessor.messagesProcessed shouldBe 2
      testLines should contain (sourceLineBackToLine(messageProcessor.lastProcessedMessage.get))
      validSourceline shouldEqual sourceLineBackToLine(messageProcessor.lastValidatedMessage.get)
      messageProcessor.messagesValidated shouldBe 1
    }
  }

  def sourceLineBackToLine(sourceLine: SourceLine): String = {
    "[1,\"" + sourceLine.stopID + "\",\"" + sourceLine.route + "\"," + sourceLine.direction + ",\"" +  sourceLine.destinationText + "\",\"" +  sourceLine.vehicleID + "\"," +  sourceLine.arrival_TimeStamp + "]"
  }
}
