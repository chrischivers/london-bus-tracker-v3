package servlet

import akka.actor.ActorSystem
import lbt.comon.{BusRoute, Commons}
import lbt.database.definitions.BusDefinitionsCollection
import lbt.database.historical.HistoricalRecordsCollection
import lbt.datasource.SourceLine
import lbt.datasource.streaming.{DataStreamProcessor, SourceLineValidator}
import lbt.historical.{HistoricalMessageProcessor, PersistAndRemoveInactiveVehicles}
import lbt.{ConfigLoader, MessageConsumer}
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

trait LbtServletTestFixture {

  implicit val actorSystem = ActorSystem("TestLbtSystem")

  val testMessagingConfig = ConfigLoader.defaultConfig.messagingConfig.copy(
    exchangeName = "test-lbt-exchange",
    historicalRecorderQueueName = "test-historical-recorder-queue-name",
    historicalRecorderRoutingKey = "test-historical-recorder-routing-key")
  val testDataSourceConfig = ConfigLoader.defaultConfig.dataSourceConfig
  val testDBConfig = ConfigLoader.defaultConfig.databaseConfig.copy(databaseName = "TestDB")
  val testDefinitionsConfig = ConfigLoader.defaultConfig.definitionsConfig
  val testHistoricalRecordsConfig = ConfigLoader.defaultConfig.historicalRecordsConfig.copy(vehicleInactivityTimeBeforePersist = 1000, numberOfLinesToCleanupAfter = 0)

  val testDefinitionsCollection = new BusDefinitionsCollection(testDefinitionsConfig, testDBConfig)

  val testHistoricalRecordsCollection = new HistoricalRecordsCollection(testDBConfig, testDefinitionsCollection)

  val testBusRoutes = List(BusRoute("3", "outbound"), BusRoute("3", "inbound")) //TODO include more randomisation on routes
  testDefinitionsCollection.refreshBusRouteDefinitionFromWeb(getOnly = Some(testBusRoutes))

  Thread.sleep(1000)

  val definitions = testDefinitionsCollection.getBusRouteDefinitions(forceDBRefresh = true)

  val historicalMessageProcessor = new HistoricalMessageProcessor(testDataSourceConfig, testHistoricalRecordsConfig, testDefinitionsCollection, testHistoricalRecordsCollection)

  val dataStreamProcessor = new DataStreamProcessor(testDataSourceConfig, testMessagingConfig)(actorSystem)


  testBusRoutes.foreach { route =>
    val vehicleReg = "V" + Random.nextInt(99999)
    val arrivalTimeMultipliers = Stream.from(1).iterator
    def generateArrivalTime = System.currentTimeMillis() + (60000 * arrivalTimeMultipliers.next())
    implicit val formats = DefaultFormats

    println("definitions: " + definitions)
    definitions(route).foreach(busStop => {
      val message = write(SourceLineValidator("[1,\"" + busStop.id + "\",\"" + route.id + "\"," + directionToInt(route.direction) + ",\"Any Place\",\"" + vehicleReg + "\"," + generateArrivalTime + "]")).getBytes
      historicalMessageProcessor.processMessage(message)
    })
  }
  Thread.sleep(1500)
  historicalMessageProcessor.vehicleActorSupervisor ! PersistAndRemoveInactiveVehicles
  Thread.sleep(500)

  def directionToInt(direction: String): Int = direction match {
    case "outbound" => 1
    case "inbound" => 2
  }
}
