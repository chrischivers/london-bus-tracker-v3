package lbt.database.definitions

import com.mongodb.casbah.Imports.{DBObject, _}
import com.mongodb.casbah.MongoCollection
import com.typesafe.scalalogging.StrictLogging
import lbt.comon.Commons.BusRouteDefinitions
import lbt.comon.{BusRoute, BusStop, Commons}
import lbt.database._
import lbt.{ConfigLoader, DatabaseConfig, DefinitionsConfig}
import net.liftweb.json.JsonAST.JArray
import net.liftweb.json.{DefaultFormats, JValue, parse}
import org.bson.json.JsonParseException

import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.util.{Failure, Success}

class BusDefinitionsCollection(defConfig: DefinitionsConfig, dbConfig: DatabaseConfig) extends DatabaseCollections with StrictLogging{

  override val db: MongoDatabase = new MongoDatabase(dbConfig)
  override val collectionName: String = defConfig.dBCollectionName
  override val fieldsVector = Vector(BUS_ROUTE_DEFINITION_DOCUMENT.ROUTE_ID, BUS_ROUTE_DEFINITION_DOCUMENT.DIRECTION, BUS_ROUTE_DEFINITION_DOCUMENT.BUS_STOP_SEQUENCE)
  override val indexKeyList = List((BUS_ROUTE_DEFINITION_DOCUMENT.ROUTE_ID, 1), (BUS_ROUTE_DEFINITION_DOCUMENT.DIRECTION, 1))
  override val uniqueIndex = true

  var numberToProcess:Long = 0

  def insertBusRouteDefinitionIntoDB(busRoute: BusRoute, busStops: List[BusStop]) = {
    incrementLogRequest(IncrementNumberInsertsRequested(1))
    BusDefinitionsDBController.insertRouteIntoDB(dBCollection, busRoute, busStops).onComplete {
      case Success(ack) =>  if (ack) incrementLogRequest(IncrementNumberInsertsCompleted(1))
                            else logger.info(s"Insert Bus Route Definition for route $busRoute was not acknowledged by DB")
      case Failure(e) => logger.info(s"Insert Bus Route Definition for route $busRoute not completed successfully", e)
    }
  }

  def getBusRouteDefinitionsFromDB = BusDefinitionsDBController.loadBusRouteDefinitionsFromDB(dBCollection) //TODO implement ttl caching

  def refreshBusRouteDefinitionFromWeb(updateNewRoutesOnly: Boolean = false, getOnly: Option[List[BusRoute]] = None): Unit = {
    implicit val formats = DefaultFormats
    logger.info("Refreshing bus route definitions from web")

    val allRoutesUrl = defConfig.sourceAllUrl
    def getSingleRouteUrl(busRoute: BusRoute) = defConfig.sourceSingleUrl.replace("#RouteID#", busRoute.id).replace("#Direction#", busRoute.direction.toString)
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
          if((getBusRouteDefinitionsFromDB.get(busRoute).isDefined && updateNewRoutesOnly) || (getOnly.isDefined && !getOnly.get.contains(busRoute))) {
            // logger.info("skipping route " + routeIDString + "and direction " + direction + " as already in DB")
          } else {
            // logger.info("processing route " + routeIDString + ", direction " + direction)
            val singleRouteJsonDataRaw = Source.fromURL(getSingleRouteUrl(busRoute)).mkString
            val singleRouteJsonDataParsed = parse(singleRouteJsonDataRaw)
            val busStopSequence = ((singleRouteJsonDataParsed \ "stopPointSequences").extract[List[JValue]].head \ "stopPoint").extract[List[JValue]]
            val busStopList = convertBusStopSequenceToBusStopList(busStopSequence)
            insertBusRouteDefinitionIntoDB(busRoute, busStopList)
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



