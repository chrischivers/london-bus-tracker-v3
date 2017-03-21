package lbt

import akka.actor.ActorSystem
import lbt.comon.BusRoute
import lbt.comon.Commons.BusRouteDefinitions
import lbt.database.definitions.BusDefinitionsTable
import lbt.database.historical.{HistoricalTable}
import lbt.datasource.streaming.DataStreamProcessor
import lbt.historical.{HistoricalSourceLineProcessor}
import lbt.servlet.LbtServlet
import org.scalatra.test.scalatest.ScalatraSuite

import scala.concurrent.ExecutionContext

class StandardTestFixture extends ScalatraSuite {

  implicit val actorSystem = ActorSystem("TestLbtSystem")
  implicit val executionContext = ExecutionContext.Implicits.global

  val testDataSourceConfig: DataSourceConfig = ConfigLoader.defaultConfig.dataSourceConfig
  val testDBConfig: DatabaseConfig = ConfigLoader.defaultConfig.databaseConfig.copy(busDefinitionsTableName = "TestDefinitions", historicalRecordsTableName = "TestHistorical")
  val testDefinitionsConfig: DefinitionsConfig = ConfigLoader.defaultConfig.definitionsConfig
  val testHistoricalRecordsConfig: HistoricalRecordsConfig = ConfigLoader.defaultConfig.historicalRecordsConfig.copy(vehicleInactivityTimeBeforePersist = 5000, numberOfLinesToCleanupAfter = 0)

  val testDefinitionsTable = new BusDefinitionsTable(testDefinitionsConfig, testDBConfig)
  val testHistoricalTable = new HistoricalTable(testDBConfig, testDefinitionsTable)

  val testBusRoute1 = BusRoute("3", "outbound") //TODO include more randomisation on routes
  val testBusRoute2 = BusRoute("3", "inbound")
  val getOnlyList = List(testBusRoute1, testBusRoute2)
  testDefinitionsTable.refreshBusRouteDefinitionFromWeb(getOnly = Some(getOnlyList), updateNewRoutesOnly = true)

  val definitions: BusRouteDefinitions = testDefinitionsTable.getBusRouteDefinitions(forceDBRefresh = true)
  println("Definitions: " + definitions)


  val historicalSourceLineProcessor = new HistoricalSourceLineProcessor(testHistoricalRecordsConfig, testDefinitionsTable, testHistoricalTable)

  val arrivalTimeMultipliers: Iterator[Int] = Stream.from(1).iterator
  def generateArrivalTime = System.currentTimeMillis() + (60000 * arrivalTimeMultipliers.next())


}
