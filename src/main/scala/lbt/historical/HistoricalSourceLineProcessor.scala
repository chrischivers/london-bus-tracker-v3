package lbt.historical

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import lbt._
import lbt.comon.{BusRoute, BusStop, Commons}
import lbt.database.definitions.BusDefinitionsCollection
import lbt.database.historical.HistoricalRecordsCollection
import lbt.datasource.SourceLine
import net.liftweb.json.{DefaultFormats, _}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scalaz.Scalaz._
import scalaz._

case class ValidatedSourceLine(busRoute: BusRoute, busStop: BusStop, destinationText: String, vehicleID: String, arrival_TimeStamp: Long)

class HistoricalSourceLineProcessor(dataSourceConfig: DataSourceConfig, historicalRecordsConfig: HistoricalRecordsConfig, definitionsCollection: BusDefinitionsCollection, historicalDbInsertPublisher: HistoricalDbInsertPublisher)(implicit actorSystem: ActorSystem, ec: ExecutionContext) extends SourceLineProcessor {

//  val cache = new SourceLineCache(dataSourceConfig.cacheTimeToLiveSeconds)

  val definitions = definitionsCollection.getBusRouteDefinitions(forceDBRefresh = true)

  val vehicleActorSupervisor = actorSystem.actorOf(Props(classOf[VehicleActorSupervisor], definitionsCollection, historicalRecordsConfig, historicalDbInsertPublisher))

  type StringValidation[T] = ValidationNel[String, T]

  implicit val formats = DefaultFormats

  override def processSourceLine(sourceLine: SourceLine) = {
    numberSourceLinesProcessed.incrementAndGet()
    lastProcessedSourceLine = Some(sourceLine)
      validateSourceLine(sourceLine) match {
        case Success(validSourceLine) => handleValidatedSourceLine(validSourceLine)
        case Failure(e) => //logger.info(s"Failed validation for sourceLine $sourceLine. Error: $e")
      }
//      cache.put(sourceLine)
  }

  def handleValidatedSourceLine(validatedSourceLine: ValidatedSourceLine) = {
    lastValidatedSourceLine = Some(validatedSourceLine)
    numberSourceLinesValidated.incrementAndGet()
    vehicleActorSupervisor ! validatedSourceLine
  }

  def validateSourceLine(sourceLine: SourceLine): StringValidation[ValidatedSourceLine] = {
    val busRoute = BusRoute(sourceLine.route, Commons.toDirection(sourceLine.direction))

    def validRouteAndStop (busRoute: BusRoute): StringValidation[BusStop] = {
      definitions.get(busRoute) match {
        case Some(stopList) => validStop(stopList)
        case None => s"Route not defined in definitions. Route ID: ${busRoute.id}. Direction: ${busRoute.direction}".failureNel
      }
    }

    def validStop(busStopList: List[BusStop]): StringValidation[BusStop] = {
      busStopList.find(stop => stop.id == sourceLine.stopID) match {
        case Some(busStop) => busStop.successNel
        case None => s"Bus Stop ${sourceLine.stopID} not defined in definitions for route ${sourceLine.route} and direction ${sourceLine.direction}".failureNel
      }
    }

//    def nonDuplicateLine(): StringValidation[Unit] = {
//      if (cache.contains(sourceLine)) "Duplicate Line Received recently".failureNel
//      else ().successNel
//    }

    def notOnIgnoreList(): StringValidation[Unit] = {
      ().successNel
    }

    def isInPast(): StringValidation[Unit] = {
      if (sourceLine.arrival_TimeStamp - System.currentTimeMillis() > 0) ().successNel
      else "Arrival time in past".failureNel
    }

      (validRouteAndStop(busRoute)
//      |@| nonDuplicateLine()
      |@| notOnIgnoreList()
      |@| isInPast()).tupled.map {
        x => ValidatedSourceLine(busRoute, x._1, sourceLine.destinationText, sourceLine.vehicleID, sourceLine.arrival_TimeStamp)
      }
  }

  def getCurrentActors = {
    implicit val timeout = Timeout(10 seconds)
    (vehicleActorSupervisor ? GetCurrentActors).mapTo[Map[String, ActorRef]]
  }

  def getArrivalRecords(vehicleReg: String, busRoute: BusRoute) = {
    implicit val timeout = Timeout(10 seconds)
    for {
      futureResult <- (vehicleActorSupervisor ? GetArrivalRecords(VehicleID(vehicleReg, busRoute))).mapTo[Future[Map[BusStop, Long]]]
      listResult <- futureResult
    } yield listResult
  }

//  def getCacheSize: Int = cache.size

//  class SourceLineCache(timeToLiveSeconds: Int) {
//    private var cache: Map[SourceLine, Long] = Map()
//
//    def put(sourceLine: SourceLine) = {
//      cache += (sourceLine -> System.currentTimeMillis())
//      cleanupCache
//    }
//
//    def contains(sourceLine: SourceLine): Boolean = {
//      cache.get(sourceLine).isDefined
//    }
//
//    def size: Int = {
//      cache.size
//    }
//
//    private def cleanupCache = {
//      val now = System.currentTimeMillis()
//      cache = cache.filter(line => (now - line._2) < timeToLiveSeconds * 1000)
//    }
//  }
}






