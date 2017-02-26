package lbt

import akka.actor.ActorSystem
import lbt.comon.{BusRoute, Start}
import lbt.database.definitions.BusDefinitionsCollection
import lbt.database.historical.HistoricalRecordsCollection
import lbt.datasource.streaming.DataStreamProcessingController
import lbt.historical.HistoricalMessageProcessor

import scala.concurrent.ExecutionContext.Implicits.global


object Main extends App {
  implicit val actorSystem = ActorSystem("LbtSystem")

  val messagingConfig = ConfigLoader.defaultConfig.messagingConfig
  val dataSourceConfig = ConfigLoader.defaultConfig.dataSourceConfig
  val dBConfig = ConfigLoader.defaultConfig.databaseConfig
  val definitionsConfig = ConfigLoader.defaultConfig.definitionsConfig
  val historicalRecordsConfig = ConfigLoader.defaultConfig.historicalRecordsConfig

  val definitionsCollection = new BusDefinitionsCollection(definitionsConfig, dBConfig)

  val historicalRecordsCollection = new HistoricalRecordsCollection(dBConfig, definitionsCollection)

  val getOnlyList = List(BusRoute("3", "outbound"), BusRoute("3", "inbound"))
  definitionsCollection.refreshBusRouteDefinitionFromWeb(getOnly = Some(getOnlyList))

  val definitions = definitionsCollection.getBusRouteDefinitions(forceDBRefresh = true)

  val messageProcessor = new HistoricalMessageProcessor(dataSourceConfig, historicalRecordsConfig, definitionsCollection, historicalRecordsCollection)

  val consumer = new MessageConsumer(messageProcessor, messagingConfig)

  val dataStreamProcessingController  = DataStreamProcessingController(messagingConfig)

  dataStreamProcessingController ! Start

}
