package lbt

import javax.servlet.ServletContext

import akka.actor.ActorSystem
import lbt.comon.{BusRoute, Start}
import lbt.database.definitions.BusDefinitionsCollection
import lbt.database.historical.HistoricalRecordsCollection
import lbt.datasource.BusDataSource.BusDataSource
import lbt.datasource.streaming.{DataStreamProcessingController, DataStreamProcessor}
import lbt.historical.HistoricalMessageProcessor
import lbt.servlet.LbtServlet

import scala.concurrent.ExecutionContext.Implicits.global


object Main extends App {
  implicit val actorSystem = ActorSystem("LbtSystem")

  val messagingConfig = ConfigLoader.defaultConfig.messagingConfig
  val dataSourceConfig = ConfigLoader.defaultConfig.dataSourceConfig
  val dBConfig = ConfigLoader.defaultConfig.databaseConfig
  val definitionsConfig = ConfigLoader.defaultConfig.definitionsConfig
  val historicalRecordsConfig = ConfigLoader.defaultConfig.historicalRecordsConfig

  val definitionsCollection = new BusDefinitionsCollection(definitionsConfig, dBConfig)
  //TODO have this accessible through user interface
 // val getOnlyList = List(BusRoute("3", "outbound"), BusRoute("3", "inbound"))
  // definitionsCollection.refreshBusRouteDefinitionFromWeb(getOnly = Some(getOnlyList))
  definitionsCollection.refreshBusRouteDefinitionFromWeb(updateNewRoutesOnly = true)
  val historicalRecordsCollection = new HistoricalRecordsCollection(dBConfig, definitionsCollection)

  val messageProcessor = new HistoricalMessageProcessor(dataSourceConfig, historicalRecordsConfig, definitionsCollection, historicalRecordsCollection)

  val consumer = new MessageConsumer(messageProcessor, messagingConfig)

  val dataSource = new BusDataSource(dataSourceConfig)

  val dataStreamProcessor  = new DataStreamProcessor(dataSource, messagingConfig)

  LbtServlet.setUpServlet

}


