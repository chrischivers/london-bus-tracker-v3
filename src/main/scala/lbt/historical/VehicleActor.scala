package lbt.historical

import akka.actor.Actor
import akka.actor.Actor.Receive
import com.typesafe.scalalogging.StrictLogging
import lbt.comon.{BusRoute, BusStop}
import lbt.dataSource.Stream.SourceLine

class VehicleActor extends Actor with StrictLogging {
  val name: String = self.path.name

  def receive = active(None, None, None)

  def active(route: Option[BusRoute], lastStop: Option[BusStop], lastStopArrivalTime: Option[Long]): Receive = {
    case vsl: ValidatedSourceLine =>
      logger.info(s"actor $name received sourceLine: $vsl")
      assert(vsl.vehicleID == name)
      //TODO do something here
      context.become(active(Some(vsl.busRoute), Some(vsl.busStop), Some(vsl.arrival_TimeStamp)))
  }
}
