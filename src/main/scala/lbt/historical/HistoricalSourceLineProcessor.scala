package lbt.historical

import java.util.concurrent.atomic.AtomicLong

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import lbt._
import lbt.comon._
import lbt.database.definitions.BusDefinitionsTable
import lbt.database.historical.HistoricalTable
import lbt.datasource.SourceLine
import net.liftweb.json.{DefaultFormats, _}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scalaz.Scalaz._
import scalaz._

case class ValidatedSourceLine(busRoute: BusRoute, busStop: BusStop, destinationText: String, vehicleReg: String, arrival_TimeStamp: Long)

class HistoricalSourceLineProcessor(historicalRecordsConfig: HistoricalRecordsConfig, definitionsTable: BusDefinitionsTable, vehicleActorSupervisor: VehicleActorSupervisor)(implicit ec: ExecutionContext) extends StrictLogging {

  val numberSourceLinesProcessed: AtomicLong = new AtomicLong(0)
  val numberSourceLinesValidated: AtomicLong = new AtomicLong(0)

  def definitions = definitionsTable.getBusRouteDefinitions()

  type StringValidation[T] = ValidationNel[String, T]

  implicit val formats = DefaultFormats

  def processSourceLine(sourceLine: SourceLine) = {
    numberSourceLinesProcessed.incrementAndGet()
      validateSourceLine(sourceLine) match {
        case Success(validSourceLine) => handleValidatedSourceLine(validSourceLine)
        case Failure(e) =>
          logger.debug(s"Failed validation for sourceLine $sourceLine. Error: $e")
      }
  }

  def handleValidatedSourceLine(validatedSourceLine: ValidatedSourceLine) = {
    numberSourceLinesValidated.incrementAndGet()
    vehicleActorSupervisor.sendValidatedLine(validatedSourceLine)
  }

  def validateSourceLine(sourceLine: SourceLine): StringValidation[ValidatedSourceLine] = {
    val busRoute = BusRoute(sourceLine.route, Commons.toDirection(sourceLine.direction))

    def validRouteAndStop (busRoute: BusRoute): StringValidation[BusStop] = {
      definitions.get(busRoute) match {
        case Some(stopList) => validStop(stopList)
        case None => s"Route not defined in definitions. Route ID: ${busRoute.name}. Direction: ${busRoute.direction}".failureNel
      }
    }

    def validStop(busStopList: List[BusStop]): StringValidation[BusStop] = {
      busStopList.find(stop => stop.stopID == sourceLine.stopID) match {
        case Some(busStop) => busStop.successNel
        case None => s"Bus Stop ${sourceLine.stopID} not defined in definitions for route ${sourceLine.route} and direction ${sourceLine.direction}".failureNel
      }
    }

    def notOnIgnoreList(): StringValidation[Unit] = ().successNel

    def isInPast(): StringValidation[Unit] = {
      //TODO is this working with clock change?
      if (sourceLine.arrival_TimeStamp - System.currentTimeMillis() > 0) ().successNel
      else "Arrival time in past".failureNel
    }
      (validRouteAndStop(busRoute)
      |@| notOnIgnoreList()
      |@| isInPast()).tupled.map {
        x => ValidatedSourceLine(busRoute, x._1, sourceLine.destinationText, sourceLine.vehicleID, sourceLine.arrival_TimeStamp)
      }
  }
}






