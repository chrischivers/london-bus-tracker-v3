package lbt

import javax.servlet.ServletContext

import akka.actor.ActorSystem
import lbt.comon.{BusRoute, Start}
import lbt.database.definitions.BusDefinitionsCollection
import lbt.database.historical.HistoricalRecordsCollection
import lbt.datasource.streaming.DataStreamProcessingController
import lbt.historical.HistoricalMessageProcessor
import lbt.servlet.LbtServlet
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.Handler
import org.scalatra.servlet.{RichServletContext, ScalatraListener}

import scala.concurrent.ExecutionContext.Implicits.global


object Main extends App {
  implicit val actorSystem = ActorSystem("LbtSystem")

  val messagingConfig = ConfigLoader.defaultConfig.messagingConfig
  val dataSourceConfig = ConfigLoader.defaultConfig.dataSourceConfig
  val dBConfig = ConfigLoader.defaultConfig.databaseConfig
  val definitionsConfig = ConfigLoader.defaultConfig.definitionsConfig
  val historicalRecordsConfig = ConfigLoader.defaultConfig.historicalRecordsConfig

  val definitionsCollection = new BusDefinitionsCollection(definitionsConfig, dBConfig)
  val getOnlyList = List(BusRoute("3", "outbound"), BusRoute("3", "inbound"))
   definitionsCollection.refreshBusRouteDefinitionFromWeb(getOnly = Some(getOnlyList))
  //definitionsCollection.refreshBusRouteDefinitionFromWeb()
  val historicalRecordsCollection = new HistoricalRecordsCollection(dBConfig, definitionsCollection)

  val messageProcessor = new HistoricalMessageProcessor(dataSourceConfig, historicalRecordsConfig, definitionsCollection, historicalRecordsCollection)

  val consumer = new MessageConsumer(messageProcessor, messagingConfig)

  val dataStreamProcessingController  = DataStreamProcessingController(messagingConfig)

  //dataStreamProcessingController ! Start

  setUpWebServlet

  def setUpWebServlet {
    val port = if (System.getenv("PORT") != null) System.getenv("PORT").toInt else 8080

    val server = new Server(port)
    val context = new WebAppContext()
    context setContextPath "/historical/"
    context.setResourceBase("src/main/webapp")
    context.addEventListener(new ScalatraListener)
    //context.addServlet(classOf[DefaultServlet], "/")
    server.setHandler(context)
    server.start
    server.join
  }
}


