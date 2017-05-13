package lbt

import akka.actor.{ActorSystem, Props}
import lbt.comon.BusRoute
import lbt.database.definitions.BusDefinitionsTable
import lbt.database.historical.HistoricalTable
import lbt.datasource.streaming.DataStreamProcessor
import lbt.historical.{HistoricalRecordsFetcher, HistoricalSourceLineProcessor, VehicleActorParent, VehicleActorSupervisor}
import lbt.servlet.LbtServlet

import scala.concurrent.ExecutionContext.Implicits.global

object Main extends App {
  implicit val actorSystem = ActorSystem("LbtSystem")

  val dataSourceConfig = ConfigLoader.defaultConfig.dataSourceConfig
  val dBConfig = ConfigLoader.defaultConfig.databaseConfig
  val definitionsConfig = ConfigLoader.defaultConfig.definitionsConfig
  val historicalRecordsConfig = ConfigLoader.defaultConfig.historicalRecordsConfig

  val definitionsTable = new BusDefinitionsTable(definitionsConfig, dBConfig)

  definitionsTable.updateBusRouteAndStopDefinitionsFromDB
  Thread.sleep(3000)
  definitionsTable.refreshBusRouteDefinitionFromWeb(updateNewRoutesOnly = true, getOnly = dataSourceConfig.getOnlyRoutes)
  Thread.sleep(3000)

  val historicalTable = new HistoricalTable(dBConfig, historicalRecordsConfig, definitionsTable)
  val vehicleActorSupervisor = new VehicleActorSupervisor(actorSystem, definitionsTable, historicalRecordsConfig, historicalTable)
  val historicalRecordsProcessor = new HistoricalRecordsFetcher(dBConfig, definitionsTable, vehicleActorSupervisor, historicalTable)
  val historicalSourceLineProcessor = new HistoricalSourceLineProcessor(historicalRecordsConfig, definitionsTable, vehicleActorSupervisor)
  val dataStreamProcessor  = new DataStreamProcessor(dataSourceConfig, historicalSourceLineProcessor)

  LbtServlet.setUpServlet

}


