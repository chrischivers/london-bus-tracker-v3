package lbt.servlet

import com.typesafe.scalalogging.StrictLogging
import lbt.Main
import lbt.comon._
import lbt.database.definitions.BusDefinitionsTable
import lbt.database.historical.{HistoricalRecordFromDb, HistoricalTable}
import lbt.datasource.streaming.{DataStreamProcessingController, DataStreamProcessor}
import lbt.historical.HistoricalSourceLineProcessor
import org.scalatra.{NotFound, Ok, ScalatraServlet}
import net.liftweb.json._
import net.liftweb.json.JsonDSL._
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.webapp.WebAppContext
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.scalatra.servlet.ScalatraListener

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success, Try}


class LbtServlet(busDefinitionsTable: BusDefinitionsTable, historicalRecordsTable: HistoricalTable, dataStreamProcessor: DataStreamProcessor, historicalMessageProcessor: HistoricalSourceLineProcessor)(implicit ec: ExecutionContext) extends ScalatraServlet with StrictLogging {

  implicit val formats = DefaultFormats
  val dtf: DateTimeFormatter = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss")

  get("/streamstart") {
    logger.info(s"/streamstart request received")
    dataStreamProcessor.start
    Ok("Started data stream processor")
  }

  get("/streamstop") {
    logger.info(s"/streamstop request received")
    dataStreamProcessor.stop
    Ok("Stopped data stream processor")
  }

  get("/routelist") {
    logger.info("/routelist request received")
    compactRender(busDefinitionsTable.getBusRouteDefinitions().map(route =>
      ("name" -> route._1.name) ~ ("direction" -> route._1.direction) ~ ("towards" -> route._2.last.stopName)))
  }

  get("/stoplist/:route/:direction") {
    val busRoute = BusRoute(params("route"), params("direction"))
    logger.info(s"/stoplist request received for $busRoute")
    busDefinitionsTable.getBusRouteDefinitions().get(busRoute) match {
      case Some(stops) => compactRender(stops map (stop =>
        ("stopID" -> stop.stopID) ~ ("stopName" -> stop.stopName) ~ ("longitude" -> stop.longitude) ~ ("latitude" -> stop.latitude)))
      case None => NotFound(s"The route $busRoute could not be found")
    }
  }

  get("/busroute/:route/:direction") {
    val busRoute = BusRoute(params("route"), params("direction"))
    val fromStopID = params.get("fromStopID")
    val toStopID = params.get("toStopID")
    val fromTime = params.get("fromTime")
    val toTime = params.get("toTime")
    val vehicleReg = params.get("vehicleID")
    val definitions = busDefinitionsTable.getBusRouteDefinitions()(busRoute)
    logger.info(s"/busroute request received for $busRoute, fromStopID $fromStopID, toStopID $toStopID, fromTime $fromTime, toTime $toTime, vehicleReg $vehicleReg")

    def getBusStop(stopID: String): Option[BusStop] = definitions.find(x => x.stopID == stopID)

    if (validateBusRoute(Some(busRoute))) {
      if (validateFromToStops(Some(busRoute), fromStopID, toStopID)) {
        if (validateFromToTime(fromTime, toTime)) {
          compactRender(historicalRecordsTable.getHistoricalRecordFromDbByBusRoute(busRoute, fromStopID, toStopID, fromTime.map(_.toLong), toTime.map(_.toLong), vehicleReg).map { rec =>
            ("busRoute" -> ("name" -> rec.busRoute.name) ~ ("direction" -> rec.busRoute.direction)) ~ ("vehicleID" -> rec.vehicleID) ~ ("stopRecords" ->
              rec.stopRecords.map(stopRec =>
                ("seqNo" -> stopRec.seqNo) ~
                  ("busStop" ->
                    ("stopID" -> stopRec.stopID) ~
                    ("stopName" -> getBusStop(stopRec.stopID).map(_.stopName).getOrElse("N/A")) ~
                      ("longitude" -> getBusStop(stopRec.stopID).map(_.longitude).getOrElse(0.0)) ~
                      ("latitude" -> getBusStop(stopRec.stopID).map(_.latitude).getOrElse(0.0))
                  ) ~ ("arrivalTime" -> stopRec.arrivalTime)))
          })
        } else NotFound(s"Invalid time window (from after to $fromTime and $toTime")
      } else NotFound(s"No records found for bus route $busRoute, from stop: $fromStopID and to stop: $toStopID")
    } else NotFound(s"No records found for bus route $busRoute")
  }

  get("/vehicle/:vehicleID") {
    val vehicleReg = params("vehicleID")
    val fromStopID = params.get("fromStopID")
    val toStopID = params.get("toStopID")
    val fromTime = params.get("fromTime")
    val toTime = params.get("toTime")
    val busRoute = for {
      route <- params.get("route")
      direction <- params.get("direction")
      busRoute = BusRoute(route, direction)
    } yield busRoute
    logger.info(s"/vehicle request received for $vehicleReg, fromStopID $fromStopID, toStopID $toStopID, fromTime $fromTime, toTime $toTime, busRoute $busRoute")

    if (validateBusRoute(busRoute)) {
      if (validateFromToStops(busRoute, fromStopID, toStopID)) {
        if (validateFromToTime(fromTime, toTime)) {
          compactRender(historicalRecordsTable.getHistoricalRecordFromDbByVehicle(vehicleReg, fromStopID, toStopID, fromTime.map(_.toLong), toTime.map(_.toLong), busRoute).map { rec =>
            ("busRoute" -> ("name" -> rec.busRoute.name) ~ ("direction" -> rec.busRoute.direction)) ~ ("vehicleID" -> rec.vehicleID) ~ ("stopRecords" ->
              rec.stopRecords.map(stopRec =>
                ("seqNo" -> stopRec.seqNo) ~
                  ("busStop" ->
                    ("stopID" -> stopRec.stopID) ~
                    ("stopName" -> busDefinitionsTable.getBusRouteDefinitions()(BusRoute(rec.busRoute.name, rec.busRoute.direction)).find(x => x.stopID == stopRec.stopID).map(_.stopName).getOrElse("N/A")) ~
                    ("longitude" -> busDefinitionsTable.getBusRouteDefinitions()(BusRoute(rec.busRoute.name, rec.busRoute.direction)).find(x => x.stopID == stopRec.stopID).map(_.longitude).getOrElse(0.0)) ~
                    ("latitude" -> busDefinitionsTable.getBusRouteDefinitions()(BusRoute(rec.busRoute.name, rec.busRoute.direction)).find(x => x.stopID == stopRec.stopID).map(_.latitude).getOrElse(0.0))
                    ) ~
                  ("arrivalTime" -> stopRec.arrivalTime)))
          })
        } else NotFound(s"Invalid time window (from after to $fromTime and $toTime")
      } else NotFound(s"No records found for vehicle reg $vehicleReg, from stop: $fromStopID and to stop: $toStopID")
    } else NotFound(s"No records found for vehicle reg $vehicleReg and bus route ${busRoute.get}")
  }

  get("/stop/:stopID") {
    val stopID = params("stopID")
    val fromTime = params.get("fromTime")
    val toTime = params.get("toTime")
    val vehicleReg = params.get("vehicleID")
    val busRoute = for {
      route <- params.get("route")
      direction <- params.get("direction")
      busRoute = BusRoute(route, direction)
    } yield busRoute
    logger.info(s"/stop request received for $stopID, fromTime $fromTime, toTime $toTime, vehiclReg $vehicleReg, busRoute $busRoute")

    if (validateBusRoute(busRoute)) {
      if (validateStopID(stopID)) {
        if (validateFromToTime(fromTime, toTime)) {

          busDefinitionsTable.getBusRouteDefinitions().flatMap(x => x._2).find(stop => stop.stopID == stopID) match {
            case Some(stopDetails) =>
              compactRender(historicalRecordsTable.getHistoricalRecordFromDbByStop(stopID, fromTime.map(_.toLong), toTime.map(_.toLong), busRoute, vehicleReg).map { rec =>
                ("busRoute" -> ("name" -> rec.busRoute.name) ~ ("direction" -> rec.busRoute.direction)) ~ ("vehicleID" -> rec.vehicleID) ~ ("stopRecords" ->
                  rec.stopRecords.map(stopRec =>
                    ("seqNo" -> stopRec.seqNo) ~
                      ("busStop" -> (
                        ("stopID" -> stopDetails.stopID) ~
                          ("stopName" -> stopDetails.stopName) ~
                          ("longitude" -> stopDetails.longitude) ~
                          ("latitude" -> stopDetails.latitude)
                        )) ~
                      ("arrivalTime" -> stopRec.arrivalTime)))
              })
            case None => NotFound(s"No records found for stopID: $stopID")
          }
        } else NotFound(s"Invalid time window (from after to $fromTime and $toTime")
      } else NotFound(s"No records found for stopID: $stopID")
    } else NotFound(s"No records found for stopID $stopID and bus route ${busRoute.get}")
  }

  get("/status") {
    <html>
      <body>
        <h1>Lbt Status</h1>
        <h2>Bus Definitions Database Table</h2>
        Number Inserts Requested = {busDefinitionsTable.numberInsertsRequested.get()}<br/>
        Number Inserts Completed = {busDefinitionsTable.numberInsertsCompleted.get()}<br/>
        Number Inserts Failed = {busDefinitionsTable.numberInsertsFailed.get()}<br/>
        Number Get Requests = {busDefinitionsTable.numberGetsRequested.get()}<br/>
        Number Delete Requests = {busDefinitionsTable.numberDeletesRequested.get()}<br/>


        <h2>Historical Records Database Table</h2>
        Number Inserts Requested = {historicalRecordsTable.numberInsertsRequested.get()}<br/>
        Number Inserts Completed = {historicalRecordsTable.numberInsertsCompleted.get()}<br/>
        Number Inserts Failed = {historicalRecordsTable.numberInsertsFailed.get()}<br/>
        Number Get Requests= {historicalRecordsTable.numberGetsRequested.get()}<br/>
        Number Delete Requests ={historicalRecordsTable.numberDeletesRequested.get()}<br/>


        <h2>Data Stream Processor</h2>
        Number Lines Processed = {Await.result(dataStreamProcessor.numberLinesProcessed, 5 seconds)}<br/>
        Number Lines Processed Since Last Restart = {Await.result(dataStreamProcessor.numberLinesProcessedSinceRestart, 5 seconds)}<br/>
        Time of Last Restart = {Await.result(dataStreamProcessor.timeOfLastRestart, 5 seconds)}<br/>
        Number of Restarts = {Await.result(dataStreamProcessor.numberOfRestarts, 5 seconds)}<br/>
        <h2>Historical Message Processor</h2>
        Number Lines Processed = {historicalMessageProcessor.numberSourceLinesProcessed.get()}<br/>
        Number Lines Validated= {historicalMessageProcessor.numberSourceLinesValidated.get()}<br/>
        Number of Vehicle Actors ={Await.result(historicalMessageProcessor.getCurrentActors, 5 seconds).size}<br/>
      </body>
    </html>
  }

  notFound {
    resourceNotFound()
  }

  private def validateBusRoute(busRoute: Option[BusRoute]): Boolean = {
    if (busRoute.isDefined) {
      busDefinitionsTable.getBusRouteDefinitions().get(busRoute.get).isDefined
    } else true
  }

  private def validateFromToStops(busRoute: Option[BusRoute], fromStopID: Option[String], toStopID: Option[String]): Boolean = {
    if (busRoute.isDefined) {
      val definition = busDefinitionsTable.getBusRouteDefinitions()(busRoute.get)
      if (fromStopID.isDefined && toStopID.isDefined) {
        if (definition.exists(stop => stop.stopID == fromStopID.get) && definition.exists(stop => stop.stopID == toStopID.get)) {
          definition.indexWhere(stop => stop.stopID == fromStopID.get) <= definition.indexWhere(stop => stop.stopID == toStopID.get)
        } else false
      } else if (fromStopID.isDefined && toStopID.isEmpty) {
        definition.exists(stop => stop.stopID == fromStopID.get)
      } else if (fromStopID.isEmpty && toStopID.isDefined) {
        definition.exists(stop => stop.stopID == toStopID.get)
      } else true
    } else true
  }

  private def validateStopID(stopID: String): Boolean = {
    busDefinitionsTable.getBusRouteDefinitions().exists(definition =>
      definition._2.exists(stop => stop.stopID == stopID)
    )
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
