package historical

import akka.actor.ActorSystem
import lbt.ConfigLoader
import lbt.comon.BusRoute
import lbt.database.definitions.BusDefinitionsCollection
import lbt.database.historical.HistoricalRecordsCollection
import lbt.datasource.streaming.DataStreamProcessor
import lbt.historical.HistoricalSourceLineProcessor
import scala.concurrent.ExecutionContext.Implicits.global

class HistoricalTestFixture(vehicleInactivityTimeout: Long = 5000) {

  implicit val actorSystem = ActorSystem("TestLbtSystem")

  val testMessagingConfig = ConfigLoader.defaultConfig.messagingConfig.copy(
    exchangeName = "test-lbt-exchange",
    historicalDBInsertQueueName = "test-historical-db-insert-queue-name",
    historicalDbRoutingKey = "test-historical-db-insert-routing-key")

  val testDataSourceConfig = ConfigLoader.defaultConfig.dataSourceConfig

  val testDBConfig = ConfigLoader.defaultConfig.databaseConfig.copy(databaseName = "TestDB")

  val testDefinitionsConfig = ConfigLoader.defaultConfig.definitionsConfig

  val testHistoricalRecordsConfig = ConfigLoader.defaultConfig.historicalRecordsConfig.copy(vehicleInactivityTimeout, numberOfLinesToCleanupAfter = 0)

  val testDefinitionsCollection = new BusDefinitionsCollection(testDefinitionsConfig, testDBConfig)

  val testHistoricalRecordsCollection = new HistoricalRecordsCollection(testDBConfig, testDefinitionsCollection)

  val testBusRoute1 = BusRoute("3", "outbound") //TODO include more randomisation on routes
  val testBusRoute2 = BusRoute("3", "inbound")
  val getOnlyList = List(testBusRoute1, testBusRoute2)
  testDefinitionsCollection.refreshBusRouteDefinitionFromWeb(getOnly = Some(getOnlyList))

  val definitions = testDefinitionsCollection.getBusRouteDefinitions(forceDBRefresh = true)

  val historicalSourceLineProcessor = new HistoricalSourceLineProcessor(testDataSourceConfig, testHistoricalRecordsConfig, testDefinitionsCollection, testMessagingConfig)

  val dataStreamProcessingControllerReal  = new DataStreamProcessor(testDataSourceConfig, testMessagingConfig, historicalSourceLineProcessor)

  val arrivalTimeMultipliers = Stream.from(1).iterator
  def generateArrivalTime = System.currentTimeMillis() + (60000 * arrivalTimeMultipliers.next())

}
