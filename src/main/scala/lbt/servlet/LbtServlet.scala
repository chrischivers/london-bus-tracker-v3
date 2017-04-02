package lbt.servlet

import com.typesafe.scalalogging.StrictLogging
import lbt.Main
import lbt.comon.Commons.BusRouteDefinitions
import lbt.comon._
import lbt.database.definitions.BusDefinitionsTable
import lbt.database.historical.{HistoricalJourneyRecordFromDb, HistoricalTable}
import lbt.datasource.streaming.{DataStreamProcessingController, DataStreamProcessor}
import lbt.historical.HistoricalSourceLineProcessor
import org.scalatra._
import net.liftweb.json._
import net.liftweb.json.JsonDSL._
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.webapp.WebAppContext
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.scalatra.servlet.ScalatraListener

import scalaz.Scalaz._
import scalaz._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Try


class LbtServlet(busDefinitionsTable: BusDefinitionsTable, historicalRecordsTable: HistoricalTable, dataStreamProcessor: DataStreamProcessor, historicalMessageProcessor: HistoricalSourceLineProcessor)(implicit ec: ExecutionContext) extends ScalatraServlet with StrictLogging {

  type StringValidation[T] = ValidationNel[String, T]
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
      case None => BadRequest(s"The route $busRoute could not be found")
    }
  }

  get("/busroute/:route/:direction") {
    val busRoute = BusRoute(params("route"), params("direction"))
    val fromStopID = params.get("fromStopID")
    val toStopID = params.get("toStopID")
    val fromArrivalTimeMillis = params.get("fromArrivalTimeMillis")
    val toArrivalTimeMillis = params.get("toArrivalTimeMillis")
    val fromArrivalTimeSecOfWeek = params.get("fromArrivalTimeSecOfWeek")
    val toArrivalTimeSecOfWeek = params.get("toArrivalTimeSecOfWeek")
    val fromJourneyStartSecOfWeek = params.get("fromJourneyStartSecOfWeek")
    val toJourneyStartSecOfWeek = params.get("toJourneyStartSecOfWeek")
    val fromJourneyStartMillis = params.get("fromJourneyStartMillis")
    val toJourneyStartMillis = params.get("toJourneyStartMillis")
    val vehicleReg = params.get("vehicleReg")
    logger.info(s"/busroute request received for $busRoute, fromStopID $fromStopID, toStopID $toStopID, fromArrivalTimeMillis $fromArrivalTimeMillis, toArrivalTimeMillis $toArrivalTimeMillis, fromArrivalTimeSecOfWeek $fromArrivalTimeSecOfWeek, toArrivalTimeSecOfWeek $toArrivalTimeSecOfWeek, fromJourneyStartSecOfWeek $fromJourneyStartSecOfWeek, toJourneyStartSecOfWeek $toJourneyStartSecOfWeek, fromJourneyStartMillis $fromJourneyStartMillis, toJourneyStartMilis $toJourneyStartMillis, vehicleReg $vehicleReg")

    (validateBusRoute(Some(busRoute)) |@|
      validateFromToStops(Some(busRoute), fromStopID, toStopID) |@|
      validateFromToArrivalTimeMillis(fromArrivalTimeMillis, toArrivalTimeMillis) |@|
      validateFromToSecOfWeek(fromArrivalTimeSecOfWeek, toArrivalTimeSecOfWeek) |@|
      validateFromToArrivalTimeMillis(fromJourneyStartMillis, toJourneyStartMillis) |@|
      validateFromToSecOfWeek(fromJourneyStartSecOfWeek, toJourneyStartSecOfWeek)).tupled.map(_ => ()) match {
      case Success(()) => compactRender(renderHistoricalJourneyRecordListToJValue(historicalRecordsTable.getHistoricalRecordFromDbByBusRoute(busRoute, fromStopID, toStopID, fromArrivalTimeMillis.map(_.toLong), toArrivalTimeMillis.map(_.toLong), fromArrivalTimeSecOfWeek.map(_.toInt), toArrivalTimeSecOfWeek.map(_.toInt), fromJourneyStartMillis.map(_.toLong), toJourneyStartMillis.map(_.toLong), fromJourneyStartSecOfWeek.map(_.toInt), toJourneyStartSecOfWeek.map(_.toInt), vehicleReg), busDefinitionsTable.getBusRouteDefinitions()))
      case Failure(e) =>
        val error = s"Unable to process /busroute request due to $e"
        logger.info(error)
        BadRequest(error)
    }
  }

  get("/vehicle/:vehicleReg") {
    val vehicleReg = params("vehicleReg")
    val stopID = params.get("stopID")
    val fromArrivalTimeMillis = params.get("fromArrivalTimeMillis")
    val toArrivalTimeMillis = params.get("toArrivalTimeMillis")
    val fromArrivalTimeSecOfWeek = params.get("fromArrivalTimeSecOfWeek")
    val toArrivalTimeSecOfWeek = params.get("toArrivalTimeSecOfWeek")
    val fromJourneyStartSecOfWeek = params.get("fromJourneyStartSecOfWeek")
    val toJourneyStartSecOfWeek = params.get("toJourneyStartSecOfWeek")
    val fromJourneyStartMillis = params.get("fromJourneyStartMillis")
    val toJourneyStartMillis = params.get("toJourneyStartMillis")
    val busRoute = for {
      route <- params.get("route")
      direction <- params.get("direction")
      busRoute = BusRoute(route, direction)
    } yield busRoute
    logger.info(s"/vehicle request received for $vehicleReg, stopID $stopID, fromArrivalTimeMillis $fromArrivalTimeMillis, toArrivalTimeMillis $toArrivalTimeMillis, fromArrivalTimeSecOfWeek $fromArrivalTimeSecOfWeek, toArrivalTimeSecOfWeek $toArrivalTimeSecOfWeek, fromJourneyStartSecOfWeek $fromJourneyStartSecOfWeek, toJourneyStartSecOfWeek $toJourneyStartSecOfWeek, fromJourneyStartMillis $fromJourneyStartMillis, toJourneyStartMilis $toJourneyStartMillis, busRoute $busRoute")

    (validateBusRoute(busRoute) |@|
      validateStopID(stopID) |@|
      validateFromToArrivalTimeMillis(fromArrivalTimeMillis, toArrivalTimeMillis) |@|
      validateFromToSecOfWeek(fromArrivalTimeSecOfWeek, toArrivalTimeMillis) |@|
      validateFromToArrivalTimeMillis(fromJourneyStartMillis, toJourneyStartMillis) |@|
      validateFromToSecOfWeek(fromJourneyStartSecOfWeek, toJourneyStartSecOfWeek)).tupled.map(_ => ()) match {
      case Success(()) => compactRender(renderHistoricalJourneyRecordListToJValue(historicalRecordsTable.getHistoricalRecordFromDbByVehicle(vehicleReg, stopID, fromArrivalTimeMillis.map(_.toLong), toArrivalTimeMillis.map(_.toLong),  fromArrivalTimeSecOfWeek.map(_.toInt), toArrivalTimeSecOfWeek.map(_.toInt), fromJourneyStartMillis.map(_.toLong), toJourneyStartMillis.map(_.toLong), fromJourneyStartSecOfWeek.map(_.toInt), toJourneyStartSecOfWeek.map(_.toInt), busRoute), busDefinitionsTable.getBusRouteDefinitions()))
      case Failure(e) =>
        val error = s"Unable to deliver /vehicle request due to $e"
        logger.info(error)
        BadRequest(error)
    }
  }

  get("/stop/:stopID") {
    val stopID = params("stopID")
    val fromArrivalTimeMillis = params.get("fromArrivalTimeMillis")
    val toArrivalTimeMillis = params.get("toArrivalTimeMillis")
    val fromArrivalSecOfWeek = params.get("fromArrivalSecOfWeek")
    val toArrivalSecOfWeek = params.get("toArrivalSecOfWeek")
    val vehicleReg = params.get("vehicleReg")
    val busRoute = for {
      route <- params.get("route")
      direction <- params.get("direction")
      busRoute = BusRoute(route, direction)
    } yield busRoute
    logger.info(s"/stop request received for $stopID, fromArrivalTimeMillis $fromArrivalTimeMillis, toArrivalTimeMillis $toArrivalTimeMillis, fromArrivalSecOfWeek $fromArrivalSecOfWeek, toArrivalSecOfWeek $toArrivalSecOfWeek, vehicleReg $vehicleReg, busRoute $busRoute")


    (validateBusRoute(busRoute) |@|
      validateStopID(Some(stopID)) |@|
      validateFromToArrivalTimeMillis(fromArrivalTimeMillis, toArrivalTimeMillis) |@|
      validateFromToSecOfWeek(fromArrivalSecOfWeek, toArrivalSecOfWeek)).tupled.map(_ => ()) match {
      case Success(()) =>  compactRender(historicalRecordsTable.getHistoricalRecordFromDbByStop(stopID, fromArrivalTimeMillis.map(_.toLong), toArrivalTimeMillis.map(_.toLong), fromArrivalSecOfWeek.map(_.toInt), toArrivalSecOfWeek.map(_.toInt), busRoute, vehicleReg).map { rec =>
        ("stopID" -> rec.stopID) ~
          ("arrivalTime" -> rec.arrivalTime) ~
          ("journey" ->
            ("busRoute" -> ("name" -> rec.journey.busRoute.name) ~ ("direction" -> rec.journey.busRoute.direction)) ~
              ("vehicleReg" -> rec.journey.vehicleReg) ~
              ("startingTimeMillis" -> rec.journey.startingTimeMillis) ~
              ("startingSecondOfWeek" -> rec.journey.startingSecondOfWeek))
      })
      case Failure(e) =>
        val error = s"Unable to process /stop request due to $e"
        logger.info(error)
        BadRequest(error)
    }
  }

  get("/status") {
    logger.info("/status request received")
    <html>
      <body>
        <h1>Lbt Status</h1>
        <h2>Bus Definitions Table</h2>
        Number Inserts Requested = {busDefinitionsTable.definitionsDBController.numberInsertsRequested.get()}<br/>
        Number Inserts Completed = {busDefinitionsTable.definitionsDBController.numberInsertsCompleted.get()}<br/>
        Number Inserts Failed = {busDefinitionsTable.definitionsDBController.numberInsertsFailed.get()}<br/>
        Number Get Requests = {busDefinitionsTable.definitionsDBController.numberGetsRequested.get()}<br/>
        Number Delete Requests = {busDefinitionsTable.definitionsDBController.numberDeletesRequested.get()}<br/>


        <h2>Historical Records Table</h2>
        Number Inserts Requested = {historicalRecordsTable.historicalDBController.numberInsertsRequested.get()}<br/>
        Number Inserts Completed = {historicalRecordsTable.historicalDBController.numberInsertsCompleted.get()}<br/>
        Number Inserts Failed = {historicalRecordsTable.historicalDBController.numberInsertsFailed.get()}<br/>
        Number Get Requests= {historicalRecordsTable.historicalDBController.numberGetsRequested.get()}<br/>
        Number Delete Requests ={historicalRecordsTable.historicalDBController.numberDeletesRequested.get()}<br/>


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

  get("/errorcount") {
    logger.info("/errorcount request received")
    <html>
      <body>
        <h1>Persist Error Counts</h1>
        <table>
        <tr>
          <td>Bus Route</td>
          <td>Count</td>
        </tr>
        {Await.result(historicalMessageProcessor.getValidationErrorMap, 5 seconds).toList.sortBy(route => route._2).reverse.map(route =>
         <tr>
            <td>{route._1.name + " - " + route._1.direction}</td>
            <td>{route._2}</td>
         </tr>
        )}
        </table>
        </body>
      </html>
  }

  notFound {
    resourceNotFound()
  }

  private def validateBusRoute(busRoute: Option[BusRoute]): StringValidation[Unit] = {
    if (busRoute.isDefined) {
      if (busDefinitionsTable.getBusRouteDefinitions().get(busRoute.get).isDefined) ().successNel
      else s"Invalid bus route $busRoute (not in definitions)".failureNel
    } else ().successNel
  }

  private def validateFromToStops(busRoute: Option[BusRoute], fromStopID: Option[String], toStopID: Option[String]): StringValidation[Unit]  = {
    if (busRoute.isDefined) {
      val definition = busDefinitionsTable.getBusRouteDefinitions()(busRoute.get)
      if (fromStopID.isDefined && toStopID.isDefined) {
        if (definition.exists(stop => stop.stopID == fromStopID.get) && definition.exists(stop => stop.stopID == toStopID.get)) {
          if (definition.indexWhere(stop => stop.stopID == fromStopID.get) <= definition.indexWhere(stop => stop.stopID == toStopID.get)) ().successNel
          else s"Index of fromStopID $fromStopID does not come before toStopID $toStopID".failureNel
        } else s"fromStopID $fromStopID and/or toStopID $toStopID not found in definitions".failureNel
      } else if (fromStopID.isDefined && toStopID.isEmpty) {
        if (definition.exists(stop => stop.stopID == fromStopID.get)) ().successNel
        else s"fromStopID $fromStopID not found in definitions".failureNel
      } else if (fromStopID.isEmpty && toStopID.isDefined) {
        if (definition.exists(stop => stop.stopID == toStopID.get)) ().successNel
        else s"toStopID $toStopID not found in definitions".failureNel
      } else ().successNel
    } else ().successNel
  }

  private def validateStopID(stopID: Option[String]): StringValidation[Unit] = {
    if (stopID.isDefined) {
      if (busDefinitionsTable.getBusRouteDefinitions().exists(definition =>
        definition._2.exists(stop => stop.stopID == stopID.get))) ().successNel
      else s"StopID $stopID does not exist in definitions".failureNel
    } else ().successNel
  }

  private def validateFromToArrivalTimeMillis(fromTime: Option[String], toTime: Option[String]): StringValidation[Unit] = {
    if (fromTime.isDefined && toTime.isDefined) {
      if (Try(fromTime.get.toLong).toOption.isDefined && Try(toTime.get.toLong).toOption.isDefined) {
        if (fromTime.get.toLong <= toTime.get.toLong) ().successNel
        else s"From time $fromTime does not come before toTime $toTime".failureNel
      } else s"Unable to convert fromTime $fromTime and toTime $toTime to Long".failureNel
    } else ().successNel
  }

  private def validateFromToSecOfWeek(fromJourneyStartSecOfWeek: Option[String], toJourneyStartSecOfWeek: Option[String]): StringValidation[Unit]  = {
    if (fromJourneyStartSecOfWeek.isDefined && toJourneyStartSecOfWeek.isDefined) {
      if (Try(fromJourneyStartSecOfWeek.get.toInt).toOption.isDefined && Try(toJourneyStartSecOfWeek.get.toInt).toOption.isDefined) {
        if (fromJourneyStartSecOfWeek.get.toInt <= toJourneyStartSecOfWeek.get.toInt &&
          fromJourneyStartSecOfWeek.get.toInt >= 0 &&
          toJourneyStartSecOfWeek.get.toInt <= 604800) ().successNel
        else s"fromJourneyStartSecOfWeek $fromJourneyStartSecOfWeek did not preceed toJourneyStartSecOfWeek $toJourneyStartSecOfWeek or did not faill within bounds (0 to 604800)".failureNel
      } else s"Unable to convert fromJourneyStartSecOfWeek $fromJourneyStartSecOfWeek and toJourneyStartSecOfWeek $toJourneyStartSecOfWeek to Int".failureNel
    } else ().successNel
  }

  private def renderHistoricalJourneyRecordListToJValue(historicalJourneyRecordFromDb: List[HistoricalJourneyRecordFromDb], busRouteDefinitions: BusRouteDefinitions): JValue = {

    def getBusStop(route: BusRoute, stopID: String): Option[BusStop] = busRouteDefinitions(route).find(x => x.stopID == stopID)

    historicalJourneyRecordFromDb.map { rec =>
      ("journey" ->
        ("busRoute" -> ("name" -> rec.journey.busRoute.name) ~ ("direction" -> rec.journey.busRoute.direction)) ~
          ("vehicleReg" -> rec.journey.vehicleReg) ~
          ("startingTimeMillis" -> rec.journey.startingTimeMillis) ~
          ("startingSecondOfWeek" -> rec.journey.startingSecondOfWeek)) ~
        ("stopRecords" ->
          rec.stopRecords.map(stopRec =>
            ("seqNo" -> stopRec.seqNo) ~
              ("busStop" ->
                  ("stopID" -> stopRec.stopID) ~
                  ("stopName" -> getBusStop(rec.journey.busRoute, stopRec.stopID).map(_.stopName).getOrElse("N/A")) ~
                  ("longitude" -> getBusStop(rec.journey.busRoute, stopRec.stopID).map(_.longitude).getOrElse(0.0)) ~
                  ("latitude" -> getBusStop(rec.journey.busRoute, stopRec.stopID).map(_.latitude).getOrElse(0.0))
                ) ~ ("arrivalTime" -> stopRec.arrivalTime)))
    }
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
