package lbt.historical

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import lbt._
import lbt.comon.{BusRoute, BusStop, Commons}
import lbt.dataSource.Stream.SourceLine
import lbt.database.definitions.BusDefinitionsCollection
import net.liftweb.json.{DefaultFormats, _}

import scala.concurrent.duration._
import scalaz.Scalaz._
import scalaz._

case class ValidatedSourceLine(busRoute: BusRoute, busStop: BusStop, destinationText: String, vehicleID: String, arrival_TimeStamp: Long)

class HistoricalMessageProcessor(dataSourceConfig: DataSourceConfig, definitionsCollection: BusDefinitionsCollection)(implicit actorSystem: ActorSystem) extends MessageProcessor {

  val cache = new SourceLineCache(dataSourceConfig.cacheTimeToLiveSeconds)

  val definitions = definitionsCollection.getBusRouteDefinitionsFromDB

  val vehicleActorSupervisor = actorSystem.actorOf(Props[VehicleActorSupervisor])

  type StringValidation[T] = Validation[String, T]

  implicit val formats = DefaultFormats

  def processMessage(message: Array[Byte]) = {
    logger.info(s"message received $message")
    messagesProcessed.incrementAndGet()
    val sourceLine = parse(new String(message, "UTF-8")).extract[SourceLine]
    println(s"message parsed: $sourceLine")
    lastProcessedMessage = Some(sourceLine)
    validateSourceLine(sourceLine) match {
      case Success(validSourceLine) => handleValidatedSourceLine(validSourceLine)
      case Failure(e) => logger.info(s"Failed validation for sourceLine $sourceLine. Error: $e")
    }
    cache.put(sourceLine)

  }

  def handleValidatedSourceLine(validatedSourceLine: ValidatedSourceLine) = {
    lastValidatedMessage = Some(validatedSourceLine)
    messagesValidated.incrementAndGet()
    vehicleActorSupervisor ! validatedSourceLine
  }

  def validateSourceLine(sourceLine: SourceLine): StringValidation[ValidatedSourceLine] = {

    val busRoute = BusRoute(sourceLine.route, Commons.toDirection(sourceLine.direction))

    def validRouteAndStop (busRoute: BusRoute): StringValidation[BusStop] = {
      definitions.get(busRoute) match {
        case Some(stopList) => validStop(stopList)
        case None => s"Route ${sourceLine.route} not defined in definitions".failure
      }
    }

    def validStop(busStopList: List[BusStop]): StringValidation[BusStop] = {
      busStopList.find(stop => stop.id == sourceLine.stopID) match {
        case Some(busStop) => busStop.success
        case None => s"Bus Stop ${sourceLine.stopID} not defined in definitions for route ${sourceLine.route}".failure
      }
    }

    def nonDuplicateLine(): StringValidation[Unit] = {
      if (cache.contains(sourceLine)) "Duplicate Line Received recently".failure
      else ().success
    }

    def notOnIgnoreList(): StringValidation[Unit] = {
      ().success
  }

      (validRouteAndStop(busRoute)
      |@| nonDuplicateLine()
      |@| notOnIgnoreList()).tupled.map {
        x => ValidatedSourceLine(busRoute, x._1, sourceLine.destinationText, sourceLine.vehicleID, sourceLine.arrival_TimeStamp)
      }
  }

  def getCurrentActors = {
    implicit val timeout = Timeout(10 seconds)
    (vehicleActorSupervisor ? GetCurrentActors).mapTo[Map[String, ActorRef]]
  }

  class SourceLineCache(timeToLiveSeconds: Int) {
    private var cache: Map[SourceLine, Long] = Map()

    def put(sourceLine: SourceLine) = {
      cache += (sourceLine -> System.currentTimeMillis())
      cleanupCache
    }

    def contains(sourceLine: SourceLine): Boolean = {
      cache.get(sourceLine).isDefined
    }

    private def cleanupCache = {
      val now = System.currentTimeMillis()
      cache = cache.filter(line => (now - line._2) < timeToLiveSeconds * 1000)
    }
  }
}






