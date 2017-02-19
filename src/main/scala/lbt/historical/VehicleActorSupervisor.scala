package lbt.historical

import akka.actor.{Actor, ActorRef, Props}
import com.typesafe.scalalogging.StrictLogging
import lbt.database.definitions.BusDefinitionsCollection

case class GetCurrentActors()

class VehicleActorSupervisor(busDefinitionsCollection: BusDefinitionsCollection) extends Actor with StrictLogging {

  def receive = active(Map.empty)

  def active(currentActors: Map[String, ActorRef]): Receive = {
    case vsl: ValidatedSourceLine => {
      currentActors.get(vsl.vehicleID) match {
        case Some(actorRef) => actorRef ! vsl
        case None =>
          val newVehicle = createNewActor(vsl.vehicleID)
          newVehicle ! vsl
          context.become(active(currentActors + (vsl.vehicleID -> newVehicle)))
      }
    }
    case GetCurrentActors => sender ! currentActors
  }

  def createNewActor(vehicleID: String): ActorRef = {
    logger.info(s"Creating new actor for vehicle ID $vehicleID")
    context.actorOf(Props(classOf[VehicleActor], busDefinitionsCollection), vehicleID)
  }
}
