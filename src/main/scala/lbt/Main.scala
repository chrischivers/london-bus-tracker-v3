package lbt

import akka.actor.ActorSystem
import lbt.database.definitions.BusDefinitionsTable
import lbt.database.historical.HistoricalTable
import lbt.datasource.streaming.DataStreamProcessor
import lbt.historical.HistoricalSourceLineProcessor
import lbt.servlet.LbtServlet
import scala.concurrent.ExecutionContext.Implicits.global

object Main extends App {
  implicit val actorSystem = ActorSystem("LbtSystem")

  val dataSourceConfig = ConfigLoader.defaultConfig.dataSourceConfig
  val dBConfig = ConfigLoader.defaultConfig.databaseConfig
  val definitionsConfig = ConfigLoader.defaultConfig.definitionsConfig
  val historicalRecordsConfig = ConfigLoader.defaultConfig.historicalRecordsConfig

  val definitionsTable = new BusDefinitionsTable(definitionsConfig, dBConfig)

  definitionsTable.updateBusRouteDefinitionsFromDB
  Thread.sleep(3000)
  definitionsTable.refreshBusRouteDefinitionFromWeb(updateNewRoutesOnly = true)
  Thread.sleep(3000)
  val historicalTable = new HistoricalTable(dBConfig, definitionsTable)

  val historicalSourceLineProcessor = new HistoricalSourceLineProcessor(historicalRecordsConfig, definitionsTable, historicalTable)

  val dataStreamProcessor  = new DataStreamProcessor(dataSourceConfig, historicalSourceLineProcessor)

  LbtServlet.setUpServlet

}


