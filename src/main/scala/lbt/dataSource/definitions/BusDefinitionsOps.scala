package lbt.dataSource.definitions

import com.typesafe.scalalogging.StrictLogging
import lbt.comon.Commons.BusRouteDefinitions
import lbt.comon._
import lbt.database.definitions.BusDefinitionsCollection
import net.liftweb.json.JsonAST._
import net.liftweb.json.{DefaultFormats, JValue, parse}
import org.bson.json.JsonParseException

import scala.collection.immutable.Seq
import scala.io.Source


class BusDefinitionsOps(busDefinitionsCollection: BusDefinitionsCollection) extends StrictLogging {

  implicit val formats = DefaultFormats

  val busRouteDefinitions: BusRouteDefinitions = busDefinitionsCollection.getBusRouteDefinitionsFromDB

  private def persistBusRouteDefinitionToDB(busRoute: BusRoute, busStops: List[BusStop]) = {
    logger.info(s"Persisting bus route $busRoute to database")
    busDefinitionsCollection.insertBusRouteDefinitionIntoDB(busRoute, busStops)
  }

  var numberToProcess = 0

  def refreshBusRouteDefinitionFromWeb(updateNewRoutesOnly: Boolean = false, getOnly: Option[List[BusRoute]] = None): Unit = {
    logger.info("Refreshing bus route definitions from web")
    val allRoutesUrl = busDefinitionsCollection.defConfig.sourceAllUrl
    def getSingleRouteUrl(busRoute: BusRoute) = busDefinitionsCollection.defConfig.sourceSingleUrl.replace("#RouteID#", busRoute.id).replace("#Direction#", busRoute.direction.toString)
    val allRouteJsonDataRaw = Source.fromURL(allRoutesUrl).mkString
    val updatedRouteList = parse(allRouteJsonDataRaw)
    val routeIDs = (updatedRouteList \ "id").extract[List[String]]
    val modeNames = (updatedRouteList \ "modeName").extract[List[String]]
    val routeSection = (updatedRouteList \ "routeSections").extract[List[JArray]]
    val directions = routeSection.map(x => (x \ "direction")).map(y => y.extractOpt[List[String]].getOrElse(List(y.extract[String])).toSet)

    val allRoutes: Seq[((String, String), Set[String])] = routeIDs zip modeNames zip directions
    println(allRoutes)
    val busRoutes: Seq[((String, String), Set[String])] = allRoutes.filter(x => x._1._2 == "bus") //.filter(x => x._1._1.as[String] == "3")
    numberToProcess = busRoutes.foldLeft(0)((acc, x) => acc + x._2.size)
    logger.info(s"number of routes to process: $numberToProcess")
    busRoutes.foreach(route => {
      route._2.foreach(directionStr => {
       // logger.info(s"Currently processing route ${route._1._1.toUpperCase} and direction $directionStr")
        try {
          val routeIDString = route._1._1.toUpperCase
          val direction = Commons.toDirection(directionStr)
          val busRoute = BusRoute(routeIDString, direction)
          if((busRouteDefinitions.get(busRoute).isDefined && updateNewRoutesOnly) || (getOnly.isDefined && !getOnly.get.contains(busRoute))) {
             // logger.info("skipping route " + routeIDString + "and direction " + direction + " as already in DB")
          } else {
           // logger.info("processing route " + routeIDString + ", direction " + direction)
            val singleRouteJsonDataRaw = Source.fromURL(getSingleRouteUrl(busRoute)).mkString
            val singleRouteJsonDataParsed = parse(singleRouteJsonDataRaw)
            val busStopSequence = ((singleRouteJsonDataParsed \ "stopPointSequences").extract[List[JValue]].head \ "stopPoint").extract[List[JValue]]
            val busStopList = convertBusStopSequenceToBusStopList(busStopSequence)
            persistBusRouteDefinitionToDB(busRoute, busStopList)
          }
        } catch {
          case e: NoSuchElementException => logger.info("No Such Element Exception for route: " + route._1._1.toUpperCase + ", and direction: " + directionStr)
          case e: JsonParseException => logger.info("JSON parse exception for route: " + route._1._1.toUpperCase + ", and direction: " + directionStr + ". " + e.printStackTrace())
          case e: Exception => logger.error("Uncaught exception " + e.printStackTrace())
        }
        numberToProcess -= 1
      })
    })

    def convertBusStopSequenceToBusStopList(busStopSequence: List[JValue]): List[BusStop] = {
      busStopSequence.map(busStop => {
        val id = (busStop \ "id").extractOpt[String].getOrElse(throw new IllegalArgumentException("No Stop ID value found in record"))
        val stopName = (busStop \ "name").extractOpt[String] match {
          case Some(jsVal) => jsVal
          case None => "No Stop Name"
        }
        val latitude = (busStop \ "lat").extractOpt[Double].getOrElse(throw new IllegalArgumentException("No Stop latitude value found in record"))
        val longitude = (busStop \ "lon").extractOpt[Double].getOrElse(throw new IllegalArgumentException("No Stop longitude value found in record"))
        BusStop(id, stopName, latitude, longitude)

      })
    }
    logger.info("Bus Route Definitions update complete")
  }
}
