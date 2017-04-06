package lbt.historical

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import lbt.HistoricalRecordsConfig
import lbt.Main.{actorSystem, definitionsTable, historicalRecordsConfig, historicalTable}
import lbt.comon.{BusRoute, BusStop}
import lbt.database.definitions.BusDefinitionsTable
import lbt.database.historical.{ArrivalRecord, HistoricalTable}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

case class GetCurrentActors()
case class GetArrivalRecords(vehicleID: VehicleActorID)
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
    case GetCurrentActors => sender ! currentActors
    case GetArrivalRecords(vehicleID) => currentActors.get(vehicleID) match {
      case Some((actorRef, _)) => sender ! (actorRef ? GetArrivalRecords(vehicleID))
      case None =>
        logger.error(s"Unable to get arrival records for $vehicleID. No such actor")
        sender ! List.empty
    }
    case GetValidationErrorMap =>
      logger.info("Received request to get validation error map")
      sender ! validationErrorCount
  }

  def createNewActor(vehicleActorID: VehicleActorID): ActorRef = {
    context.actorOf(Props(classOf[VehicleActor], vehicleActorID, historicalRecordsConfig, busDefinitionsTable, historicalTable), vehicleActorID.toString)
  }
}

class VehicleActorSupervisor(actorSystem: ActorSystem, definitionsTable: BusDefinitionsTable, historicalRecordsConfig: HistoricalRecordsConfig, historicalTable: HistoricalTable)(implicit ec: ExecutionContext) {

  private val vehicleActorParent = actorSystem.actorOf(Props(classOf[VehicleActorParent], definitionsTable, historicalRecordsConfig, historicalTable))

  def sendValidatedLine(validatedSourceLine: ValidatedSourceLine) = {
    vehicleActorParent ! validatedSourceLine
  }

  def getLiveArrivalRecords(busRoute: BusRoute, vehicleReg: String) = {
    implicit val timeout = Timeout(10 seconds)
    for {
      futureResult <- (vehicleActorParent ? GetArrivalRecords(VehicleActorID(vehicleReg, busRoute))).mapTo[Future[Map[BusStop, Long]]]
      listResult <- futureResult
    } yield listResult
  }

  def getValidationErrorMap = {
    implicit val timeout = Timeout(10 seconds)
    (vehicleActorParent ? GetValidationErrorMap).mapTo[Map[BusRoute, Int]]
  }

  def getCurrentActors = {
    implicit val timeout = Timeout(10 seconds)
    (vehicleActorParent ? GetCurrentActors).mapTo[Map[String, ActorRef]]
  }

  def persistAndRemoveInactiveVehicles = {
    vehicleActorParent ! PersistAndRemoveInactiveVehicles
  }
}
