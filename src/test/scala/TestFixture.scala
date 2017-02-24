import akka.actor.ActorSystem
import lbt.comon.{BusRoute, Outbound}
import lbt.dataSource.Stream.DataStreamProcessingController
import lbt.database.definitions.BusDefinitionsCollection
import lbt.database.historical.HistoricalRecordsCollection
import lbt.historical.HistoricalMessageProcessor
import lbt.{ConfigLoader, MessageConsumer}

import scala.concurrent.ExecutionContext.Implicits.global

class TestFixture {

  implicit val actorSystem = ActorSystem("TestLbtSystem")

  val testMessagingConfig = ConfigLoader.defaultConfig.messagingConfig.copy(
    exchangeName = "test-lbt-exchange",
    historicalRecorderQueueName = "test-historical-recorder-queue-name",
    historicalRecorderRoutingKey = "test-historical-recorder-routing-key")

  val testDataSourceConfig = ConfigLoader.defaultConfig.dataSourceConfig

  val testDBConfig = ConfigLoader.defaultConfig.databaseConfig.copy(databaseName = "TestDB")

  val testDefinitionsConfig = ConfigLoader.defaultConfig.definitionsConfig

  val testDefinitionsCollection = new BusDefinitionsCollection(testDefinitionsConfig, testDBConfig)

  val testHistoricalRecordsCollection = new HistoricalRecordsCollection(testDBConfig)

  val testBusRoute = BusRoute("3", Outbound()) //TODO include more randomisation on routes
  val getOnlyList = List(testBusRoute)
  testDefinitionsCollection.refreshBusRouteDefinitionFromWeb(getOnly = Some(getOnlyList))
  Thread.sleep(5000)

  val definitions = testDefinitionsCollection.getBusRouteDefinitionsFromDB

  val messageProcessor = new HistoricalMessageProcessor(testDataSourceConfig, testDefinitionsCollection, testHistoricalRecordsCollection)
  val consumer = new MessageConsumer(messageProcessor, testMessagingConfig)

  val dataStreamProcessingControllerReal  = DataStreamProcessingController(testMessagingConfig)

  val arrivalTimeMultipliers = Stream.from(1).iterator
  def generateArrivalTime = System.currentTimeMillis() + (60000 * arrivalTimeMultipliers.next())
}
