package lbt

import akka.actor.ActorSystem
import lbt.comon.{BusRoute, BusStop}
import lbt.comon.Commons.BusRouteDefinitions
import lbt.database.definitions.BusDefinitionsTable
import lbt.database.historical.{HistoricalTable, Journey}
import lbt.datasource.streaming.DataStreamProcessor
import lbt.historical.HistoricalSourceLineProcessor
import lbt.servlet.LbtServlet
import org.scalatra.test.scalatest.ScalatraSuite

import scala.concurrent.ExecutionContext

case class TransmittedIncomingHistoricalRecord(journey: Journey, stopRecords: List[TransmittedIncomingHistoricalArrivalRecord])
case class TransmittedIncomingHistoricalArrivalRecord(seqNo: Int, busStop: BusStop, arrivalTime: Long)
case class TransmittedIncomingHistoricalStopRecord(stopID: String, arrivalTime: Long, journey: Journey)
case class TransmittedBusRouteWithTowards(name: String, direction: String, towards: String)



class StandardTestFixture extends ScalatraSuite {

  implicit val actorSystem = ActorSystem("TestLbtSystem")
  implicit val executionContext = ExecutionContext.Implicits.global

  val testDataSourceConfig: DataSourceConfig = ConfigLoader.defaultConfig.dataSourceConfig
  val testDBConfig: DatabaseConfig = ConfigLoader.defaultConfig.databaseConfig.copy(busDefinitionsTableName = "TestDefinitions", historicalRecordsTableName = "TestHistorical")
  val testDefinitionsConfig: DefinitionsConfig = ConfigLoader.defaultConfig.definitionsConfig
  val testHistoricalRecordsConfig: HistoricalRecordsConfig = ConfigLoader.defaultConfig.historicalRecordsConfig.copy(vehicleInactivityTimeBeforePersist = 5000, numberOfLinesToCleanupAfter = 0, toleranceForFuturePredictions = 600000)

  val testDefinitionsTable = new BusDefinitionsTable(testDefinitionsConfig, testDBConfig)
  val testHistoricalTable = new HistoricalTable(testDBConfig, testDefinitionsTable)

  val testBusRoute1 = BusRoute("3", "outbound") //TODO include more randomisation on routes
  val testBusRoute2 = BusRoute("3", "inbound")
  val getOnlyList = List(testBusRoute1, testBusRoute2)
  testDefinitionsTable.refreshBusRouteDefinitionFromWeb(getOnly = Some(getOnlyList), updateNewRoutesOnly = true)
  Thread.sleep(2000)
  val definitions: BusRouteDefinitions = testDefinitionsTable.getBusRouteDefinitions(forceDBRefresh = true)
  println("Definitions: " + definitions)


  val historicalSourceLineProcessor = new HistoricalSourceLineProcessor(testHistoricalRecordsConfig, testDefinitionsTable, testHistoricalTable)

  val now = System.currentTimeMillis() + 5000
  val arrivalTimeAdder: Iterator[Int] = Stream.from(1).iterator
  def generateArrivalTime = now + (arrivalTimeAdder.next() * 1000)


}
