package lbt.historical

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import lbt.HistoricalRecordsConfig
import lbt.Main.{actorSystem, definitionsTable, historicalRecordsConfig, historicalTable}
import lbt.comon.{BusRoute, BusStop, Commons}
import lbt.database.definitions.BusDefinitionsTable
import lbt.database.historical._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

case class GetCurrentActors()
case class GetArrivalRecords(vehicleID: VehicleActorID)
case class GetValidatedArrivalRecords(vehicleID: VehicleActorID)
case class GetLiveArrivalRecordsForBusRoute(busRoute: BusRoute)
case class GetLiveArrivalRecordsForVehicleId(vehicleActorID: VehicleActorID)
case class PersistToDB()
case class PersistAndRemoveInactiveVehicles()
case class ValidationError(busRoute: BusRoute, reason: String)
case class GetValidationErrorMap()
case class VehicleActorID(vehicleReg: String, busRoute: BusRoute) {
  override def toString: String = vehicleReg + "-" + busRoute.name + "-" + busRoute.direction
}


class VehicleActorParent(busDefinitionsTable: BusDefinitionsTable, historicalRecordsConfig: HistoricalRecordsConfig, historicalTable: HistoricalTable) extends Actor with StrictLogging {
  logger.info("Vehicle Actor Supervisor Actor Created")
  implicit val timeout = Timeout(10 seconds)

  def receive = active(Map.empty, historicalRecordsConfig.numberOfLinesToCleanupAfter, Map.empty)

  def active(currentActors: Map[VehicleActorID, (ActorRef, Long)], linesUntilCleanup: Int, validationErrorCount: Map[BusRoute, Int]): Receive = {
    case vsl: ValidatedSourceLine =>
      val vehicleActorID = VehicleActorID(vsl.vehicleReg, vsl.busRoute)
      currentActors.get(vehicleActorID) match {
        case Some((actorRef, _)) =>
          actorRef ! vsl
          context.become(active(currentActors + (vehicleActorID -> (actorRef, System.currentTimeMillis())), linesUntilCleanup - 1, validationErrorCount))
        case None =>
          val newVehicle = createNewActor(vehicleActorID)
          newVehicle ! vsl
          context.become(active(currentActors + (vehicleActorID -> (newVehicle, System.currentTimeMillis())), linesUntilCleanup - 1, validationErrorCount))
      }
      if (linesUntilCleanup <= 0) self ! PersistAndRemoveInactiveVehicles
    case PersistAndRemoveInactiveVehicles =>
      val currentTime = System.currentTimeMillis()
      val currentActorsSplit = currentActors.partition {
        case (_, (_, lastActivity)) => currentTime - lastActivity > historicalRecordsConfig.vehicleInactivityTimeBeforePersist
      }
      currentActorsSplit._1.map {
        case (_, (actorRef, _)) => actorRef
      }.foreach { actorRef =>
        actorRef ! PersistToDB
        actorRef ! PoisonPill
      }
      context.become(active(currentActorsSplit._2, historicalRecordsConfig.numberOfLinesToCleanupAfter, validationErrorCount))
    case ValidationError(route, _) =>
      val currentErrorCount = validationErrorCount.get(route)
      context.become(active(currentActors, linesUntilCleanup, validationErrorCount + (route -> currentErrorCount.map(count => count + 1).getOrElse(1))))
    case GetCurrentActors =>
      logger.info("Get Current Actors request received")
      sender ! currentActors
    case gar @ GetArrivalRecords(vehicleID) =>
      logger.info(s"GetArrivalRecords request received for $vehicleID")
      currentActors.get(vehicleID) match {
      case Some((actorRef, _)) => sender ! (actorRef ? gar)
      case None =>
        logger.error(s"Unable to get arrival records for $vehicleID. No such actor")
        sender ! List.empty
    }
    case gvar @ GetValidatedArrivalRecords(vehicleID) =>
      logger.info(s"GetValidatedArrivalRecords request received for $vehicleID")
      currentActors.get(vehicleID) match {
      case Some((actorRef, _)) => sender ! (actorRef ? gvar).mapTo[List[ArrivalRecord]]
      case None =>
        logger.error(s"Unable to get validated arrival records for $vehicleID. No such actor")
        sender ! List.empty
    }
    case GetValidationErrorMap =>
      logger.info("Received request to get validation error map")
      sender ! validationErrorCount
  }

  def createNewActor(vehicleActorID: VehicleActorID): ActorRef = {
    logger.debug(s"New actor created $vehicleActorID")
    context.actorOf(Props(classOf[VehicleActor], vehicleActorID, historicalRecordsConfig, busDefinitionsTable, historicalTable), vehicleActorID.toString)
  }
}

class VehicleActorSupervisor(actorSystem: ActorSystem, definitionsTable: BusDefinitionsTable, historicalRecordsConfig: HistoricalRecordsConfig, historicalTable: HistoricalTable)(implicit ec: ExecutionContext) extends StrictLogging {

  private val vehicleActorParent = actorSystem.actorOf(Props(classOf[VehicleActorParent], definitionsTable, historicalRecordsConfig, historicalTable))

  def sendValidatedLine(validatedSourceLine: ValidatedSourceLine) = {
    vehicleActorParent ! validatedSourceLine
  }

  def getLiveArrivalRecords(vehicleActorID: VehicleActorID): Future[Map[BusStop, Long]] = {
    implicit val timeout = Timeout(10 seconds)
    for {
      futureResult <- (vehicleActorParent ? GetArrivalRecords(vehicleActorID)).mapTo[Future[Map[BusStop, Long]]]
      listResult <- futureResult
    } yield listResult
  }

  def getLiveValidatedArrivalRecords(vehicleActorID: VehicleActorID): Future[List[ArrivalRecord]] = {
    implicit val timeout = Timeout(10 seconds)
    for {
      futureResult <- (vehicleActorParent ? GetValidatedArrivalRecords(vehicleActorID)).mapTo[Future[List[ArrivalRecord]]]
      listResult <- futureResult
    } yield listResult
  }

  def getLiveArrivalRecordsForRoute(busRoute: BusRoute): Future[List[HistoricalJourneyRecord]] = {
    for {
      currentActors <- getCurrentActors
      actorsForRoute = currentActors.filter(_._1.busRoute == busRoute).keys
      listOfFutures = actorsForRoute.map(vehicleID => getLiveValidatedArrivalRecords(vehicleID).map(y => vehicleID -> y)).toList
      sequence <- Future.sequence(listOfFutures)
    } yield {
      val filteredRecords = sequence.filter(rec => rec._2.nonEmpty)
      val historicalJourneyRecords = filteredRecords.map(record =>
        HistoricalJourneyRecord(
          Journey(record._1.busRoute, record._1.vehicleReg, record._2.head.arrivalTime, Commons.getSecondsOfWeek(record._2.head.arrivalTime)),
          Source("Live"),
          record._2
        ))
      historicalJourneyRecords
    }
  }

  def getLiveArrivalRecordsForVehicle(vehicleReg: String): Future[List[HistoricalJourneyRecord]] = {
    for {
      currentActors <- getCurrentActors
      actorsForVehicle = currentActors.filter(_._1.vehicleReg == vehicleReg).keys
      result <- Future.sequence(actorsForVehicle.map(vehicleID => getLiveValidatedArrivalRecords(vehicleID).map(y => vehicleID -> y)).toList)
    } yield {
      result.map(record =>
        HistoricalJourneyRecord(
          Journey(record._1.busRoute, record._1.vehicleReg, record._2.head.arrivalTime, Commons.getSecondsOfWeek(record._2.head.arrivalTime)),
          Source("Live"),
          record._2
        ))
    }
  }
  //TODO combine these into one

  def getLiveArrivalRecordsForStop(filteredRoutesContainingStop: List[BusRoute], stopID: String): Future[List[HistoricalStopRecord]] = {
    for {
      currentActors <- getCurrentActors
      actorsForRoute = currentActors.filter(vehicle => filteredRoutesContainingStop.contains(vehicle._1.busRoute)).keys
      allRecords <- Future.sequence(actorsForRoute.map(getLiveArrivalRecords))
      allRecordsWithVehicleIDs = actorsForRoute.zip(allRecords)
      result = allRecordsWithVehicleIDs.map(record => (record._1, record._2.head._2, record._2.find(_._1.stopID == stopID))).filter(_._3.isDefined).toList
    } yield {
      result.map(record =>
        HistoricalStopRecord(
          record._3.get._1.stopID,
          record._3.get._2,
          Journey(record._1.busRoute, record._1.vehicleReg, record._2, Commons.getSecondsOfWeek(record._2)),
          Source("Live")
        ))
    }
  }

  def getValidationErrorMap = {
    implicit val timeout = Timeout(30 seconds)
    (vehicleActorParent ? GetValidationErrorMap).mapTo[Map[BusRoute, Int]]
  }

  def getCurrentActors: Future[Map[VehicleActorID, (ActorRef, Long)]] = {
    implicit val timeout = Timeout(30 seconds)
    (vehicleActorParent ? GetCurrentActors).mapTo[Map[VehicleActorID, (ActorRef, Long)]]
  }

  def persistAndRemoveInactiveVehicles = {
    vehicleActorParent ! PersistAndRemoveInactiveVehicles
  }
}
