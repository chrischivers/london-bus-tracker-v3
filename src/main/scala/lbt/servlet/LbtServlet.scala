package lbt.servlet

import lbt.Main
import lbt.comon.BusRoute
import lbt.database.definitions.BusDefinitionsCollection
import lbt.database.historical.{HistoricalRecordFromDb, HistoricalRecordsCollection}
import lbt.datasource.streaming.{DataStreamProcessingController, DataStreamProcessor}
import org.scalatra.{NotFound, Ok, ScalatraServlet}
import net.liftweb.json._
import net.liftweb.json.JsonDSL._
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.servlet.ScalatraListener

import scala.util.Try


class LbtServlet(busDefinitionsCollection: BusDefinitionsCollection, historicalRecordsCollection: HistoricalRecordsCollection, dataStreamProcessor: DataStreamProcessor) extends ScalatraServlet {

  implicit val formats = DefaultFormats

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
