package lbt

import akka.actor.ActorSystem
import lbt.comon.{BusRoute, Direction, RouteID}
import lbt.comon.Commons.BusRouteDefinitions
import lbt.database.definitions.BusDefinitionsCollection
import lbt.database.historical.{HistoricalRecordsCollection, HistoricalRecordsCollectionConsumer}
import lbt.datasource.streaming.DataStreamProcessor
import lbt.historical.{HistoricalDbInsertPublisher, HistoricalSourceLineProcessor}
import lbt.servlet.LbtServlet
import org.scalatra.test.scalatest.ScalatraSuite

import scala.concurrent.ExecutionContext

class StandardTestFixture extends ScalatraSuite {

  implicit val actorSystem = ActorSystem("TestLbtSystem")
  implicit val executionContext = ExecutionContext.Implicits.global

  val testMessagingConfig: MessagingConfig = ConfigLoader.defaultConfig.messagingConfig.copy(
    exchangeName = "test-lbt-exchange",
    historicalDBInsertQueueName = "test-lbt.historical-db-insert-queue-name",
    historicalDbRoutingKey = "test-lbt.historical-db-insert-routing-key")
  val testDataSourceConfig: DataSourceConfig = ConfigLoader.defaultConfig.dataSourceConfig
  val testDBConfig: DatabaseConfig = ConfigLoader.defaultConfig.databaseConfig.copy(databaseName = "TestDB")
  val testDefinitionsConfig: DefinitionsConfig = ConfigLoader.defaultConfig.definitionsConfig
  val testHistoricalRecordsConfig: HistoricalRecordsConfig = ConfigLoader.defaultConfig.historicalRecordsConfig.copy(vehicleInactivityTimeBeforePersist = 5000, numberOfLinesToCleanupAfter = 0)

  val testDefinitionsCollection = new BusDefinitionsCollection(testDefinitionsConfig, testDBConfig)
  val testHistoricalRecordsCollection = new HistoricalRecordsCollection(testDBConfig, testDefinitionsCollection)

  val testBusRoute1 = BusRoute(RouteID("3"), Direction("outbound")) //TODO include more randomisation on routes
  val testBusRoute2 = BusRoute(RouteID("3"), Direction("inbound"))
  val getOnlyList = List(testBusRoute1, testBusRoute2)
  testDefinitionsCollection.refreshBusRouteDefinitionFromWeb(getOnly = Some(getOnlyList))

  val definitions: BusRouteDefinitions = testDefinitionsCollection.getBusRouteDefinitions(forceDBRefresh = true)
  println("Definitions: " + definitions)

  val testHistoricalRecordsCollectionConsumer = new HistoricalRecordsCollectionConsumer(testMessagingConfig, testHistoricalRecordsCollection)

  val testHistoricalDbInsertPublisher = new HistoricalDbInsertPublisher(testMessagingConfig)
  val historicalSourceLineProcessor = new HistoricalSourceLineProcessor(testHistoricalRecordsConfig, testDefinitionsCollection, testHistoricalDbInsertPublisher)

  val arrivalTimeMultipliers: Iterator[Int] = Stream.from(1).iterator
  def generateArrivalTime = System.currentTimeMillis() + (60000 * arrivalTimeMultipliers.next())


}
