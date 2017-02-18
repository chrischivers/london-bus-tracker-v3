package lbt.historical

import java.util.concurrent.atomic.AtomicLong

import lbt._
import lbt.comon.{BusRoute, Commons}
import lbt.dataSource.Stream.SourceLine
import lbt.database.definitions.BusDefinitionsCollection
import net.liftweb.json.DefaultFormats
import net.liftweb.json._

import scala.util.Random
import scalaz.Scalaz._
import scalaz.Validation


class HistoricalMessageProcessor(dataSourceConfig: DataSourceConfig, definitionsCollection: BusDefinitionsCollection) extends MessageProcessor {

  val cache = new SourceLineCache(dataSourceConfig.cacheTimeToLiveSeconds)

  type StringValidation[T] = Validation[String, T]

  implicit val formats = DefaultFormats

  def processMessage(message: Array[Byte]) = {
    logger.info(s"message received $message")
    messagesProcessed.incrementAndGet()
    val sourceLine = parse(new String(message, "UTF-8")).extract[SourceLine]
    println(s"message parsed: $sourceLine")
    lastProcessedMessage = Some(sourceLine)
    if (validateSourceLine(sourceLine).isSuccess) {
      lastValidatedMessage = Some(sourceLine)
      messagesValidated.incrementAndGet()
      cache.put(sourceLine)
    }
  }

  def validateSourceLine(sourceLine: SourceLine): StringValidation[_] = {

    val busRoute = BusRoute(sourceLine.route, Commons.toDirection(sourceLine.direction))
    val definitions = definitionsCollection.getBusRouteDefinitionsFromDB

    def validRoute(busRoute: BusRoute): StringValidation[BusRoute] = {
      definitions.get(busRoute) match {
        case Some(stopList) => stopList.exists(stop => stop.id == sourceLine.stopID) match {
          case true => busRoute.success
          case false => s"Bus Stop ${sourceLine.stopID} not defined in definitions for route ${sourceLine.route}".failure
        }
        case None => s"Route ${sourceLine.route} not defined in definitions".failure
      }
    }

      def nonDuplicateLine(sourceLine: SourceLine): StringValidation[SourceLine] = {
        if (cache.contains(sourceLine)) "Duplicate Line Received recently".failure
        else sourceLine.success
      }

      def notOnIgnoreList(busRoute: BusRoute): StringValidation[BusRoute] = {
      busRoute.success
    }

   (validRoute(busRoute)
      |@| nonDuplicateLine(sourceLine)
      |@| notOnIgnoreList(busRoute)).tupled
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






