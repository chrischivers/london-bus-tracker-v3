package lbt.database.definitions

import com.typesafe.scalalogging.StrictLogging
import lbt.comon.Commons.BusRouteDefinitions
import lbt.comon._
import lbt.database._
import lbt.{DatabaseConfig, DefinitionsConfig}
import net.liftweb.json.JsonAST.JArray
import net.liftweb.json.{DefaultFormats, JValue, parse}

import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContext
import scala.io.Source

class BusDefinitionsTable(defConfig: DefinitionsConfig, dbConfig: DatabaseConfig)(implicit ec: ExecutionContext) extends DatabaseTables with StrictLogging {

  private val definitionsDBController = new DefinitionsDynamoDBController(dbConfig)(ec)

  private var numberToProcess: Long = 0
  private var definitionsCache: BusRouteDefinitions = Map.empty

  def insertBusRouteDefinitionIntoDB(busRoute: BusRoute, busStops: List[BusStop]) = {
    numberGetsRequested.incrementAndGet()
    definitionsDBController.insertRouteIntoDB(busRoute, busStops)
    numberInsertsCompleted.incrementAndGet()
  }

  def getBusRouteDefinitions(forceDBRefresh: Boolean = false): BusRouteDefinitions = {
    if (definitionsCache.isEmpty || forceDBRefresh) updateBusRouteDefinitionsFromDB
    definitionsCache
  }

  def updateBusRouteDefinitionsFromDB: Unit = {
    numberGetsRequested.incrementAndGet()
    definitionsCache = definitionsDBController.loadBusRouteDefinitionsFromDB
    logger.info("Bus Route Definitions cache updated from database")
  }

  def refreshBusRouteDefinitionFromWeb(updateNewRoutesOnly: Boolean = false, getOnly: Option[List[BusRoute]] = None): Unit = {
    implicit val formats = DefaultFormats
    logger.info("Refreshing bus route definitions from web")

    val allRoutesUrl = defConfig.sourceAllUrl

    def getSingleRouteUrl(busRoute: BusRoute) = defConfig.sourceSingleUrl.replace("#RouteID#", busRoute.name).replace("#Direction#", busRoute.direction)

    val allRouteJsonDataRaw = Source.fromURL(allRoutesUrl).mkString
    val updatedRouteList = parse(allRouteJsonDataRaw)
    val routeIDs = (updatedRouteList \ "id").extract[List[String]]
    val modeNames = (updatedRouteList \ "modeName").extract[List[String]]
    val routeSection = (updatedRouteList \ "routeSections").extract[List[JArray]]
    val directions = routeSection.map(x => (x \ "direction")).map(y => y.extractOpt[List[String]].getOrElse(List(y.extract[String])).toSet)

    val allRoutes: Seq[((String, String), Set[String])] = routeIDs zip modeNames zip directions
    val busRoutes: Seq[((String, String), Set[String])] = allRoutes.filter(x => x._1._2 == "bus")
    numberToProcess = busRoutes.foldLeft(0)((acc, x) => acc + x._2.size)
    logger.info(s"number of routes to process: $numberToProcess")
    busRoutes.foreach(route => {
      route._2.foreach(direction => {
        try {
          val routeID = route._1._1.toUpperCase
          val busRoute = BusRoute(routeID, direction)
          if (getOnly.isDefined && !getOnly.get.contains(busRoute)) {
            //TODO What if it is in DB but incomplete?
            logger.info("skipping route " + routeID + " and direction " + direction + " as route is not in GetOnly parameter")
          } else if (getBusRouteDefinitions().get(busRoute).isDefined && updateNewRoutesOnly) {
            logger.info("skipping route " + routeID + " and direction " + direction + " as already in DB")
          } else {
            logger.info("processing route " + routeID + ", direction " + direction)
            val singleRouteJsonDataRaw = Source.fromURL(getSingleRouteUrl(busRoute)).mkString
            val singleRouteJsonDataParsed = parse(singleRouteJsonDataRaw)
            val busStopSequence = ((singleRouteJsonDataParsed \ "stopPointSequences").extract[List[JValue]].head \ "stopPoint").extract[List[JValue]]
            val busStopList = convertBusStopSequenceToBusStopList(busStopSequence)
            insertBusRouteDefinitionIntoDB(busRoute, busStopList)
          }
        } catch {
          case e: NoSuchElementException => logger.info("No Such Element Exception for route: " + route._1._1.toUpperCase + ", and direction: " + direction)
          case e: Exception => logger.error("Uncaught exception ", e)
        }
        numberToProcess -= 1
        logger.info(s"number of routes left to process: $numberToProcess")
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
    updateBusRouteDefinitionsFromDB
    logger.info("Bus Route Definitions cache updated from database")
  }

  def deleteTable = definitionsDBController.deleteTable

}



