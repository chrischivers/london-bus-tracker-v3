package lbt.historical

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import lbt.{HistoricalRecordsConfig, MessagingConfig}
import lbt.comon.{BusRoute, VehicleReg}
import lbt.database.definitions.BusDefinitionsCollection
import lbt.database.historical.HistoricalRecordsCollection

import scala.concurrent.duration._

case class GetCurrentActors()

case class GetArrivalRecords(vehicleID: VehicleActorID)

case class PersistToDB()

case class PersistAndRemoveInactiveVehicles()

case class VehicleActorID(vehicleReg: VehicleReg, busRoute: BusRoute) {
  override def toString: String = vehicleReg.value + "-" + busRoute.id.value + "-" + busRoute.direction.value
}

class VehicleActorSupervisor(busDefinitionsCollection: BusDefinitionsCollection, historicalRecordsConfig: HistoricalRecordsConfig, historicalDbInsertPublisher: HistoricalDbInsertPublisher) extends Actor with StrictLogging {
  logger.info("Vehicle Actor Supervisor Actor Created")
  implicit val timeout = Timeout(10 seconds)

  def receive = active(Map.empty, historicalRecordsConfig.numberOfLinesToCleanupAfter)

  def active(currentActors: Map[VehicleActorID, (ActorRef, Long)], linesUntilCleanup: Int): Receive = {
    case vsl: ValidatedSourceLine => {
      val vehicleActorID = VehicleActorID(vsl.vehicleReg, vsl.busRoute)
      currentActors.get(vehicleActorID) match {
        case Some((actorRef, _)) =>
          actorRef ! vsl
          context.become(active(currentActors + (vehicleActorID -> (actorRef, System.currentTimeMillis())), linesUntilCleanup - 1))
        case None =>
          val newVehicle = createNewActor(vehicleActorID)
          newVehicle ! vsl
          context.become(active(currentActors + (vehicleActorID -> (newVehicle, System.currentTimeMillis())), linesUntilCleanup - 1))
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
//      logger.info("Checking for inactive vehicles, persisting and deleting...")
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

  def createNewActor(vehicleActorID: VehicleActorID): ActorRef = {
   // logger.info(s"Creating new actor for vehicle ID $vehicleID")
    context.actorOf(Props(classOf[VehicleActor], vehicleActorID, historicalRecordsConfig, busDefinitionsCollection, historicalDbInsertPublisher), vehicleActorID.toString)
  }
}
