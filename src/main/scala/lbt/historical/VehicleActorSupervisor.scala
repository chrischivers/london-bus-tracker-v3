package lbt.historical

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import lbt.HistoricalRecordsConfig
import lbt.comon.BusRoute
import lbt.database.definitions.BusDefinitionsCollection
import lbt.database.historical.HistoricalRecordsCollection

import scala.concurrent.duration._

case class GetCurrentActors()

case class GetArrivalRecords(vehicleID: VehicleID)

case class PersistToDB()

case class PersistAndRemoveInactiveVehicles()

case class VehicleID(vehicleReg: String, busRoute: BusRoute) {
  override def toString: String = vehicleReg + "-" + busRoute.id + "-" + busRoute.direction
}

class VehicleActorSupervisor(busDefinitionsCollection: BusDefinitionsCollection, historicalRecordsCollection: HistoricalRecordsCollection, historicalRecordsConfig: HistoricalRecordsConfig) extends Actor with StrictLogging {

  implicit val timeout = Timeout(10 seconds)

  def receive = active(Map.empty, historicalRecordsConfig.numberOfLinesToCleanupAfter)

  def active(currentActors: Map[VehicleID, (ActorRef, Long)], linesUntilCleanup: Int): Receive = {
    case vsl: ValidatedSourceLine => {
      val vehicleID = VehicleID(vsl.vehicleID, vsl.busRoute)
      currentActors.get(vehicleID) match {
        case Some((actorRef, _)) =>
          actorRef ! vsl
          context.become(active(currentActors + (vehicleID -> (actorRef, System.currentTimeMillis())), linesUntilCleanup - 1))
        case None =>
          val newVehicle = createNewActor(vehicleID)
          newVehicle ! vsl
          context.become(active(currentActors + (vehicleID -> (newVehicle, System.currentTimeMillis())), linesUntilCleanup - 1))
      }
      if (linesUntilCleanup <= 0) self ! PersistAndRemoveInactiveVehicles
    }
    case GetCurrentActors => sender ! currentActors
    case GetArrivalRecords(vehicleID) => currentActors.get(vehicleID) match {
      case Some((actorRef, _)) => sender ! (actorRef ? GetArrivalRecords(vehicleID))
      case None =>
        logger.error(s"Unable to get arrival records for $vehicleID. No such actor")
        sender ! List.empty
    }
    case PersistAndRemoveInactiveVehicles =>
      logger.info("Cleaning up inactive vehicles")
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
      context.become(active(currentActorsSplit._2, historicalRecordsConfig.numberOfLinesToCleanupAfter))
  }

  def createNewActor(vehicleID: VehicleID): ActorRef = {
    logger.info(s"Creating new actor for vehicle ID $vehicleID")
    context.actorOf(Props(classOf[VehicleActor], vehicleID.vehicleReg, historicalRecordsConfig, busDefinitionsCollection, historicalRecordsCollection), vehicleID.toString)
  }
}
