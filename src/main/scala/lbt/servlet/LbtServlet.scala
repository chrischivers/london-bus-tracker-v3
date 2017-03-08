package lbt.servlet

import lbt.Main
import lbt.comon.BusRoute
import lbt.database.definitions.BusDefinitionsCollection
import lbt.database.historical.{HistoricalRecordFromDb, HistoricalRecordsCollection, HistoricalRecordsCollectionConsumer}
import lbt.datasource.streaming.{DataStreamProcessingController, DataStreamProcessor}
import lbt.historical.{HistoricalDbInsertPublisher, HistoricalSourceLineProcessor}
import org.scalatra.{NotFound, Ok, ScalatraServlet}
import net.liftweb.json._
import net.liftweb.json.JsonDSL._
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.servlet.ScalatraListener

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success, Try}


class LbtServlet(busDefinitionsCollection: BusDefinitionsCollection, historicalRecordsCollection: HistoricalRecordsCollection, dataStreamProcessor: DataStreamProcessor, historicalMessageProcessor: HistoricalSourceLineProcessor, historicalRecordsCollectionConsumer: HistoricalRecordsCollectionConsumer, historicalDbInsertPublisher: HistoricalDbInsertPublisher)(implicit ec: ExecutionContext) extends ScalatraServlet {

  implicit val formats = DefaultFormats

  get("/status") {
    <html>
      <body>
        <h1>Lbt Status</h1>
        <h2>Bus Definitions</h2>
        Number Inserts Requested = {busDefinitionsCollection.numberInsertsRequested.get()}<br />
        Number Inserts Completed = {busDefinitionsCollection.numberInsertsCompleted.get()}<br />
        Number Inserts Failed = {busDefinitionsCollection.numberInsertsFailed.get()}<br />
        Number Get Requests= {busDefinitionsCollection.numberGetsRequested.get()}<br />
        Number Delete Requests = {busDefinitionsCollection.numberDeletesRequested.get()}<br />
      <br />
      <h2>Historical Records Collection</h2>
        Number Insert Messages Published =
        Number Insert Messages Consumed = {historicalRecordsCollectionConsumer.getNumberMessagesConsumed}
        Number Inserts Requested = {historicalRecordsCollection.numberInsertsRequested.get()}<br />
        Number Inserts Completed = {historicalRecordsCollection.numberInsertsCompleted.get()}<br />
        Number Inserts Failed = {historicalRecordsCollection.numberInsertsFailed.get()}<br />
        Number Get Requests= {historicalRecordsCollection.numberGetsRequested.get()}<br />
        Number Delete Requests = {historicalRecordsCollection.numberDeletesRequested.get()}<br />
      <br></br>
        <h2>Data Stream Processor</h2>
        Number Lines Processed = {Await.result(dataStreamProcessor.numberLinesProcessed, 5 seconds)}<br />
        Number Lines Processed Since Restart = {Await.result(dataStreamProcessor.numberLinesProcessedSinceRestart, 5 seconds)}<br />
        <br></br>
        <h2>Historical Message Processor</h2>
        Number Lines Processed = {historicalMessageProcessor.numberSourceLinesProcessed.get()}<br />
        Number Lines Validated= {historicalMessageProcessor.numberSourceLinesValidated.get()}<br />
        Number of Vehicle Actors = {Await.result(historicalMessageProcessor.getCurrentActors, 5 seconds).size}<br />
        <br />
      </body>
    </html>
  }

  get("/streamstart") {
    dataStreamProcessor.start
    Ok("Started data stream processor")
  }

  get("/streamstop") {
    dataStreamProcessor.stop
    Ok("Stopped data stream processor")
  }

  get("/routelist") {
    compactRender(busDefinitionsCollection.getBusRouteDefinitions().keys.toList.map(key =>
      ("id" -> key.id) ~ ("direction" -> key.direction)))
  }

  get("/:route/:direction/stoplist") {
    val busRoute = BusRoute(params("route"), params("direction"))
    busDefinitionsCollection.getBusRouteDefinitions().get(busRoute) match {
      case Some(stops) => compactRender(stops map (stop =>
        ("id" -> stop.id) ~ ("name" -> stop.name) ~ ("longitude" -> stop.longitude) ~ ("latitude" -> stop.latitude)))
      case None => NotFound(s"The route $busRoute could not be found")
    }
  }

  get("/:route/:direction") {
    val busRoute = BusRoute(params("route"), params("direction"))
    val fromStopID = params.get("fromStopID")
    val toStopID = params.get("toStopID")
    val fromTime = params.get("fromTime")
    val toTime = params.get("toTime")
    val vehicleReg = params.get("vehicleID")
    if (validateBusRoute(busRoute)) {
      if (validateFromToStops(busRoute, fromStopID, toStopID)) {
        if (validateFromToTime(fromTime, toTime)) {
          compactRender(historicalRecordsCollection.getHistoricalRecordFromDB(busRoute, fromStopID, toStopID, fromTime.map(_.toLong), toTime.map(_.toLong), vehicleReg).map { rec =>
            ("busRoute" -> ("id" -> rec.busRoute.id) ~ ("direction" -> rec.busRoute.direction)) ~ ("vehicleID" -> rec.vehicleID) ~ ("stopRecords" ->
              rec.stopRecords.map(stopRec =>
                ("seqNo" -> stopRec.seqNo) ~ ("stopID" -> stopRec.stopID) ~ ("arrivalTime" -> stopRec.arrivalTime)))
          })
        } else NotFound(s"Invalid time window (from after to $fromTime and $toTime")
      } else NotFound(s"No records found for bus route $busRoute, from stop: $fromStopID and to stop: $toStopID")
    } else NotFound(s"No records found for bus route $busRoute")
  }

  notFound {
    resourceNotFound()
  }

    private def validateBusRoute(busRoute: BusRoute): Boolean = {
      busDefinitionsCollection.getBusRouteDefinitions().get(busRoute).isDefined
    }

    private def validateFromToStops(busRoute: BusRoute, fromStopID: Option[String], toStopID: Option[String]): Boolean = {
      val definition = busDefinitionsCollection.getBusRouteDefinitions()(busRoute)
      if (fromStopID.isDefined && toStopID.isDefined) {
        if (definition.exists(stop => stop.id == fromStopID.get) && definition.exists(stop => stop.id == toStopID.get)) {
          definition.indexWhere(stop => stop.id == fromStopID.get) <= definition.indexWhere(stop => stop.id == toStopID.get)
        } else false
      } else if (fromStopID.isDefined && toStopID.isEmpty) {
        definition.exists(stop => stop.id == fromStopID.get)
      } else if (fromStopID.isEmpty && toStopID.isDefined) {
        definition.exists(stop => stop.id == toStopID.get)
      } else true
    }

  private def validateFromToTime(fromTime: Option[String], toTime: Option[String]): Boolean = {
    if (fromTime.isDefined && toTime.isDefined) {
      if (Try(fromTime.get.toLong).toOption.isDefined && Try(toTime.get.toLong).toOption.isDefined) {
        fromTime.get < toTime.get
      } else false
    } else true
  }
}

object LbtServlet {

  def setUpServlet: Unit = {
    val port = if (System.getenv("PORT") != null) System.getenv("PORT").toInt else 8080

    val server = new Server(port)
    val context = new WebAppContext()
    context setContextPath "/historical/"
    context.setResourceBase("src/main/webapp")
    context.addEventListener(new ScalatraListener)
    context.addServlet(classOf[DefaultServlet], "/")
    server.setHandler(context)
    server.start
    server.join
  }
}
