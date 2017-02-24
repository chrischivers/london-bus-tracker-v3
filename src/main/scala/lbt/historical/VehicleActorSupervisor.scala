package lbt.historical

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import lbt.database.definitions.BusDefinitionsCollection
import lbt.database.historical.HistoricalRecordsCollection

import scala.concurrent.duration._

case class GetCurrentActors()
case class GetArrivalRecords(vehicleID: String)

class VehicleActorSupervisor(busDefinitionsCollection: BusDefinitionsCollection, historicalRecordsCollection: HistoricalRecordsCollection) extends Actor with StrictLogging {

  implicit val timeout = Timeout(10 seconds)

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
    case GetArrivalRecords(vehicleID) => currentActors.get(vehicleID) match {
        case Some(actorRef) => sender ! (actorRef ? GetArrivalRecords(vehicleID))
        case None =>
          logger.error(s"Unable to get arrival records for $vehicleID. No such actor")
          sender ! List.empty
      }
  }

  def createNewActor(vehicleID: String): ActorRef = {
    logger.info(s"Creating new actor for vehicle ID $vehicleID")
    context.actorOf(Props(classOf[VehicleActor], busDefinitionsCollection, historicalRecordsCollection), vehicleID)
  }
}
