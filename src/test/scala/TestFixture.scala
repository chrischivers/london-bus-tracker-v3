import akka.actor.ActorSystem
import lbt.comon.{BusRoute, Outbound}
import lbt.dataSource.Stream.DataStreamProcessingController
import lbt.dataSource.definitions.BusDefinitionsOps
import lbt.{ConfigLoader, MessageConsumer}
import lbt.database.definitions.BusDefinitionsCollection
import lbt.historical.HistoricalMessageProcessor

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

  val testBusRoute = BusRoute("3", Outbound()) //TODO include more randomisation on routes
  val getOnlyList = List(testBusRoute)
  new BusDefinitionsOps(testDefinitionsCollection).refreshBusRouteDefinitionFromWeb(getOnly = Some(getOnlyList))
  Thread.sleep(5000)

  val definitions = testDefinitionsCollection.getBusRouteDefinitionsFromDB

  val messageProcessor = new HistoricalMessageProcessor(testDataSourceConfig, testDefinitionsCollection)
  val consumer = new MessageConsumer(messageProcessor, testMessagingConfig)

  val dataStreamProcessingControllerReal  = DataStreamProcessingController(testMessagingConfig)
}
