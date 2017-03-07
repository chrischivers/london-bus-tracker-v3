package lbt

import javax.servlet.ServletContext

import akka.actor.ActorSystem
import lbt.comon.{BusRoute, Start}
import lbt.database.definitions.BusDefinitionsCollection
import lbt.database.historical.{HistoricalRecordsCollection, HistoricalRecordsCollectionConsumer}
import lbt.datasource.streaming.{DataStreamProcessingController, DataStreamProcessor}
import lbt.historical.HistoricalSourceLineProcessor
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
  val getOnlyList = List(BusRoute("3", "outbound"), BusRoute("3", "inbound"))
   definitionsCollection.refreshBusRouteDefinitionFromWeb(getOnly = Some(getOnlyList))
//  definitionsCollection.refreshBusRouteDefinitionFromWeb(updateNewRoutesOnly = true)
  Thread.sleep(3000)
  definitionsCollection.updateBusRouteDefinitionsFromDB
  val historicalRecordsCollection = new HistoricalRecordsCollection(dBConfig, definitionsCollection)
  val historicalRecordsCollectionConsumer = new HistoricalRecordsCollectionConsumer(messagingConfig, historicalRecordsCollection)

  val historicalSourceLineProcessor = new HistoricalSourceLineProcessor(dataSourceConfig, historicalRecordsConfig, definitionsCollection, messagingConfig)

  val dataStreamProcessor  = new DataStreamProcessor(dataSourceConfig, messagingConfig, historicalSourceLineProcessor)

  LbtServlet.setUpServlet

}


