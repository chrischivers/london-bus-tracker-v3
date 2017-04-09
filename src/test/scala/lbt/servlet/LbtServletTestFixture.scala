package lbt.servlet

import akka.actor.ActorSystem
import lbt.ConfigLoader
import lbt.comon.BusRoute
import lbt.comon.Commons.BusRouteDefinitions
import lbt.database.definitions.BusDefinitionsTable
import lbt.database.historical.HistoricalTable
import lbt.datasource.streaming.{DataStreamProcessor, SourceLineValidator}
import lbt.historical.{HistoricalRecordsFetcher, HistoricalSourceLineProcessor, PersistAndRemoveInactiveVehicles, VehicleActorSupervisor}
import net.liftweb.json.DefaultFormats

import scala.concurrent.ExecutionContext
import scala.util.Random

trait LbtServletTestFixture {

  implicit val actorSystem = ActorSystem("TestLbtSystem")
  implicit val executionContext = ExecutionContext.Implicits.global

  val testDataSourceConfig = ConfigLoader.defaultConfig.dataSourceConfig
  val testDBConfig = ConfigLoader.defaultConfig.databaseConfig.copy(busDefinitionsTableName = "TestDefinitions", historicalRecordsTableName = "TestHistorical")
  val testDefinitionsConfig = ConfigLoader.defaultConfig.definitionsConfig
  val testHistoricalRecordsConfig = ConfigLoader.defaultConfig.historicalRecordsConfig.copy(vehicleInactivityTimeBeforePersist = 1000, numberOfLinesToCleanupAfter = 0)

  val testDefinitionsTable = new BusDefinitionsTable(testDefinitionsConfig, testDBConfig)
  val testHistoricalTable = new HistoricalTable(testDBConfig, testDefinitionsTable)
  val vehicleActorSupervisor = new VehicleActorSupervisor(actorSystem, testDefinitionsTable, testHistoricalRecordsConfig, testHistoricalTable)
  val historicalRecordsFetcher = new HistoricalRecordsFetcher(testDBConfig, testDefinitionsTable, vehicleActorSupervisor, testHistoricalTable)

  val testBusRoutes = List(BusRoute("3", "outbound"), BusRoute("3", "inbound")) //TODO include more randomisation on routes
  testDefinitionsTable.refreshBusRouteDefinitionFromWeb(getOnly = Some(testBusRoutes :+ BusRoute("521", "inbound")), updateNewRoutesOnly = true)

  Thread.sleep(2000)

  val definitions: BusRouteDefinitions = testDefinitionsTable.getBusRouteDefinitions(forceDBRefresh = true)

  val historicalSourceLineProcessor = new HistoricalSourceLineProcessor(testHistoricalRecordsConfig, testDefinitionsTable, vehicleActorSupervisor)

  val dataStreamProcessor = new DataStreamProcessor(testDataSourceConfig, historicalSourceLineProcessor)(actorSystem, executionContext)

  val now = System.currentTimeMillis()

  val vehicleReg = "V" + Random.nextInt(99999)
  testBusRoutes.foreach { route =>

    val arrivalTimeIncrementer = Stream.from(1).iterator
    def generateArrivalTime = now + (arrivalTimeIncrementer.next() * 1000)
    implicit val formats = DefaultFormats

    println("definitions: " + definitions)
    //Complete route
    definitions(route).foreach(busStop => {
      val message = SourceLineValidator("[1,\"" + busStop.stopID + "\",\"" + route.name + "\"," + directionToInt(route.direction) + ",\"Any Place\",\"" + vehicleReg + "\"," + generateArrivalTime + "]").get
      historicalSourceLineProcessor.processSourceLine(message)
    })
  }
  Thread.sleep(1500)
  vehicleActorSupervisor.persistAndRemoveInactiveVehicles
  Thread.sleep(500)

  def directionToInt(direction: String): Int = direction match {
    case "outbound" => 1
    case "inbound" => 2
  }
}
