package lbt

import akka.actor.ActorSystem
import lbt.comon.{BusRoute, Inbound, Outbound, Start}
import lbt.dataSource.Stream.DataStreamProcessingController
import lbt.database.definitions.BusDefinitionsCollection
import lbt.historical.HistoricalMessageProcessor


object Main extends App {
  implicit val actorSystem = ActorSystem("LbtSystem")

  val messagingConfig = ConfigLoader.defaultConfig.messagingConfig
  val dataSourceConfig = ConfigLoader.defaultConfig.dataSourceConfig
  val dBConfig = ConfigLoader.defaultConfig.databaseConfig
  val definitionsConfig = ConfigLoader.defaultConfig.definitionsConfig

  val definitionsCollection = new BusDefinitionsCollection(definitionsConfig, dBConfig)

  val getOnlyList = List(BusRoute("3", Outbound()), BusRoute("3", Inbound()))
  definitionsCollection.refreshBusRouteDefinitionFromWeb(getOnly = Some(getOnlyList))

  val definitions = definitionsCollection.getBusRouteDefinitionsFromDB

  val messageProcessor = new HistoricalMessageProcessor(dataSourceConfig, definitionsCollection)

  val consumer = new MessageConsumer(messageProcessor, messagingConfig)

  val dataStreamProcessingController  = DataStreamProcessingController(messagingConfig)

  dataStreamProcessingController ! Start

}
