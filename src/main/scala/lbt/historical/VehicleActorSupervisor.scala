package lbt.historical

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import lbt.HistoricalRecordsConfig
import lbt.database.definitions.BusDefinitionsCollection
import lbt.database.historical.HistoricalRecordsCollection

import scala.concurrent.duration._

case class GetCurrentActors()

case class GetArrivalRecords(vehicleID: String)

case class PersistToDB()

case class CleanUpInactiveVehicles()

class VehicleActorSupervisor(busDefinitionsCollection: BusDefinitionsCollection, historicalRecordsCollection: HistoricalRecordsCollection, historicalRecordsConfig: HistoricalRecordsConfig) extends Actor with StrictLogging {

  implicit val timeout = Timeout(10 seconds)

  def receive = active(Map.empty, historicalRecordsConfig.numberOfLinesToCleanupAfter)

  def active(currentActors: Map[String, (ActorRef, Long)], linesUntilCleanup: Int): Receive = {
    case vsl: ValidatedSourceLine => {
      currentActors.get(vsl.vehicleID) match {
        case Some((actorRef, _)) =>
          actorRef ! vsl
          context.become(active(currentActors + (vsl.vehicleID -> (actorRef, System.currentTimeMillis())), linesUntilCleanup - 1))
        case None =>
          val newVehicle = createNewActor(vsl.vehicleID)
          newVehicle ! vsl
          context.become(active(currentActors + (vsl.vehicleID -> (newVehicle, System.currentTimeMillis())), linesUntilCleanup - 1))
      }
      if (linesUntilCleanup <= 0) self ! CleanUpInactiveVehicles
    }
    case GetCurrentActors => sender ! currentActors
    case GetArrivalRecords(vehicleID) => currentActors.get(vehicleID) match {
      case Some((actorRef, _)) => sender ! (actorRef ? GetArrivalRecords(vehicleID))
      case None =>
        logger.error(s"Unable to get arrival records for $vehicleID. No such actor")
        sender ! List.empty
    }
    case CleanUpInactiveVehicles =>
      logger.info("Cleaning up inactive vehicles")
      val currentTime = System.currentTimeMillis()
      val currentActorsSplit = currentActors.partition {
        case (_, (_, lastActivity)) => currentTime - lastActivity > historicalRecordsConfig.vehicleInactivityTimeout
      }
      currentActorsSplit._1.map {
        case (_, (actorRef, _)) => actorRef
      }.foreach { actorRef =>
        actorRef ! PersistToDB
        actorRef ! PoisonPill
      }
      context.become(active(currentActorsSplit._2, historicalRecordsConfig.numberOfLinesToCleanupAfter))
  }

  def createNewActor(vehicleID: String): ActorRef = {
    logger.info(s"Creating new actor for vehicle ID $vehicleID")
    context.actorOf(Props(classOf[VehicleActor], busDefinitionsCollection, historicalRecordsCollection), vehicleID)
  }
}
