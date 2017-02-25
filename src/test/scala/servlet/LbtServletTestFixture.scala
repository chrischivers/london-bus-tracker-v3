package servlet

import akka.actor.ActorSystem
import lbt.comon.BusRoute
import lbt.database.definitions.BusDefinitionsCollection
import lbt.database.historical.HistoricalRecordsCollection
import lbt.{ConfigLoader, MessageConsumer}

import scala.concurrent.ExecutionContext.Implicits.global

trait LbtServletTestFixture {

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

  val testBusRoutes = List(BusRoute("3", "outbound"), BusRoute("3", "inbound")) //TODO include more randomisation on routes
  testDefinitionsCollection.refreshBusRouteDefinitionFromWeb(getOnly = Some(testBusRoutes))

  val definitions = testDefinitionsCollection.getBusRouteDefinitionsFromDB


}
