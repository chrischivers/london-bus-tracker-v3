package lbt.servlet

import akka.actor.ActorSystem
import lbt.ConfigLoader
import lbt.comon.{BusRoute, Direction, RouteID, VehicleReg}
import lbt.comon.Commons.BusRouteDefinitions
import lbt.database.definitions.BusDefinitionsCollection
import lbt.database.historical.{HistoricalRecordsCollection, HistoricalRecordsCollectionConsumer}
import lbt.datasource.streaming.{DataStreamProcessor, SourceLineValidator}
import lbt.historical.{HistoricalDbInsertPublisher, HistoricalSourceLineProcessor, PersistAndRemoveInactiveVehicles}
import net.liftweb.json.DefaultFormats

import scala.concurrent.ExecutionContext
import scala.util.Random

trait LbtServletTestFixture {

  implicit val actorSystem = ActorSystem("TestLbtSystem")
  implicit val executionContext = ExecutionContext.Implicits.global

  val testMessagingConfig = ConfigLoader.defaultConfig.messagingConfig.copy(
    exchangeName = "test-lbt-exchange",
    historicalDBInsertQueueName = "test-lbt.historical-db-insert-queue-name",
    historicalDbRoutingKey = "test-lbt.historical-db-insert-routing-key")
  val testDataSourceConfig = ConfigLoader.defaultConfig.dataSourceConfig
  val testDBConfig = ConfigLoader.defaultConfig.databaseConfig.copy(databaseName = "TestDB")
  val testDefinitionsConfig = ConfigLoader.defaultConfig.definitionsConfig
  val testHistoricalRecordsConfig = ConfigLoader.defaultConfig.historicalRecordsConfig.copy(vehicleInactivityTimeBeforePersist = 1000, numberOfLinesToCleanupAfter = 0)

  val testDefinitionsCollection = new BusDefinitionsCollection(testDefinitionsConfig, testDBConfig)

  val testHistoricalRecordsCollection = new HistoricalRecordsCollection(testDBConfig, testDefinitionsCollection)

  val testBusRoutes = List(BusRoute(RouteID("3"), Direction("outbound")), BusRoute(RouteID("3"), Direction("inbound"))) //TODO include more randomisation on routes
  testDefinitionsCollection.refreshBusRouteDefinitionFromWeb(getOnly = Some(testBusRoutes))

  Thread.sleep(1000)

  val definitions: BusRouteDefinitions = testDefinitionsCollection.getBusRouteDefinitions(forceDBRefresh = true)

  val historicalRecordsCollectionConsumer = new HistoricalRecordsCollectionConsumer(testMessagingConfig, testHistoricalRecordsCollection)

  val historicalDbInsertPublisher = new HistoricalDbInsertPublisher(testMessagingConfig)

  val historicalSourceLineProcessor = new HistoricalSourceLineProcessor(testHistoricalRecordsConfig, testDefinitionsCollection, historicalDbInsertPublisher)

  val dataStreamProcessor = new DataStreamProcessor(testDataSourceConfig, testMessagingConfig, historicalSourceLineProcessor)(actorSystem, executionContext)

  val vehicleReg = VehicleReg("V" + Random.nextInt(99999))
  testBusRoutes.foreach { route =>
    val arrivalTimeMultipliers = Stream.from(1).iterator
    def generateArrivalTime = System.currentTimeMillis() + (60000 * arrivalTimeMultipliers.next())
    implicit val formats = DefaultFormats

    println("definitions: " + definitions)
    definitions(route).foreach(busStop => {
      val message = SourceLineValidator("[1,\"" + busStop.id + "\",\"" + route.id + "\"," + directionToInt(route.direction.value) + ",\"Any Place\",\"" + vehicleReg + "\"," + generateArrivalTime + "]").get
      historicalSourceLineProcessor.processSourceLine(message)
    })
  }
  Thread.sleep(1500)
  historicalSourceLineProcessor.vehicleActorSupervisor ! PersistAndRemoveInactiveVehicles
  Thread.sleep(500)

  def directionToInt(direction: String): Int = direction match {
    case "outbound" => 1
    case "inbound" => 2
  }
}
