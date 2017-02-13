package lbt.dataSource.definitions

import com.typesafe.scalalogging.StrictLogging
import lbt.DefinitionsConfig
import lbt.comon._
import lbt.dataSource.definitions.BusDefinitionsOps.BusRouteDefinitions
import lbt.database.DbCollections
import lbt.database.definitions.BusDefinitionsCollection
import org.bson.json.JsonParseException
import play.api.libs.json.{JsValue, Json}

import scala.io.Source

object BusDefinitionsOps {
  type BusRouteDefinitions = Map[BusRoute, List[BusStop]]
  def loadBusDefinitionsOps(busDefinitionsCollection: BusDefinitionsCollection) = {
    new BusDefinitionsOps(busDefinitionsCollection)
  }
}

class BusDefinitionsOps(busDefinitionsCollection: BusDefinitionsCollection) extends StrictLogging {

  val busRouteDefinitions: BusRouteDefinitions = retrieveAllBusRouteDefinitionsFromDB

  private def retrieveAllBusRouteDefinitionsFromDB = busDefinitionsCollection.getBusRouteDefinitionsFromDB

  private def persistBusRouteDefinitionsToDB(busRoute: BusRoute, busStops: List[BusStop]) = busDefinitionsCollection.insertBusRouteDefinitionIntoDB(busRoute, busStops)

  var numberToProcess = 0

  def refreshBusRouteDefinitionFromWeb(updateNewRoutesOnly: Boolean = false, getOnly: Option[List[BusRoute]] = None): Unit = {
    val allRoutesUrl = busDefinitionsCollection.defConfig.sourceAllUrl
    def getSingleRouteUrl(busRoute: BusRoute) = busDefinitionsCollection.defConfig.sourceSingleUrl.replace("#RouteID#", busRoute.id).replace("#Direction#", busRoute.direction.toString)
    val allRouteJsonDataRaw = Source.fromURL(allRoutesUrl).mkString
    val updatedRouteList = Json.parse(allRouteJsonDataRaw)
    val routeID = updatedRouteList \\ "id"
    val modeName = updatedRouteList \\ "modeName"
    val routeSection = updatedRouteList \\ "routeSections"
    val directions = routeSection.map(x => x \\ "direction")
    val allRoutes: Seq[((JsValue, JsValue), Seq[JsValue])] = routeID zip modeName zip directions
    val busRoutes: Seq[((JsValue, JsValue), Seq[JsValue])] = allRoutes.filter(x => x._1._2.as[String] == "bus") //.filter(x => x._1._1.as[String] == "3")
    numberToProcess = busRoutes.foldLeft(0)((acc, x) => acc + x._2.length)

    busRoutes.foreach(route => {
      route._2.foreach(directionStr => {
        logger.info(s"Currently processing route ${route._1._1.as[String].toUpperCase} and direction $directionStr")
        try {
          val routeIDString = route._1._1.as[String].toUpperCase
          val direction = Commons.directionStrToDirection(directionStr.as[String])
          val busRoute = BusRoute(routeIDString, direction)
          if((busRouteDefinitions.get(busRoute).isDefined && updateNewRoutesOnly) || (getOnly.isDefined && !getOnly.get.contains(busRoute))) {
            logger.info("skipping route " + routeIDString + "and direction " + direction + " as already in DB")
          } else {
            logger.info("processing route " + routeIDString + ", direction " + direction)
            val singleRouteJsonDataRaw = Source.fromURL(getSingleRouteUrl(busRoute)).mkString
            val singleRouteJsonDataParsed = Json.parse(singleRouteJsonDataRaw)
            val busStopSequences = singleRouteJsonDataParsed \\ "stopPointSequences"
            val busStopSequence = (busStopSequences.head \\ "stopPoint").head.as[List[JsValue]]
            val busStopList = convertBusStopSequenceToBusStopList(busStopSequence)
            persistBusRouteDefinitionsToDB(busRoute, busStopList)
          }
        } catch {
          case e: NoSuchElementException => logger.info("No Such Element Exception for route: " + route._1._1.as[String].toUpperCase + ", and direction: " + directionStr)
          case e: JsonParseException => logger.info("JSON parse exception for route: " + route._1._1.as[String].toUpperCase + ", and direction: " + directionStr + ". " + e.printStackTrace())
          case e: Exception => logger.error("Uncaught exception " + e.printStackTrace())
        }
        numberToProcess -= 1
      })
    })

    def convertBusStopSequenceToBusStopList(busStopSequence: List[JsValue]): List[BusStop] = {
      busStopSequence.map(busStop => {
        println(busStop)
        val id = (busStop \\ "id").headOption.getOrElse(throw new IllegalArgumentException("No Stop ID value found in record")).as[String]
        val stopName = (busStop \\ "name").headOption match {
          case Some(jsVal) => jsVal.as[String]
          case None => "No Stop Name"
        }
        val latitude = (busStop \\ "lat").headOption.getOrElse(throw new IllegalArgumentException("No Stop latitude value found in record")).as[BigDecimal]
        val longitude = (busStop \\ "lon").headOption.getOrElse(throw new IllegalArgumentException("No Stop longitude value found in record")).as[BigDecimal]
        BusStop(id, stopName, latitude.toString(), longitude.toString())

      })
    }
    logger.info("Bus Route Definitions update complete")
  }
}
