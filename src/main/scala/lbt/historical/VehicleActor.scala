package lbt.historical

import akka.actor.Actor
import com.typesafe.scalalogging.StrictLogging
import lbt.comon.{BusRoute, BusStop}
import lbt.database.definitions.BusDefinitionsCollection

case class StopArrivalRecord(busStop: BusStop, stopArrivalTime: Long)

class VehicleActor(busDefinitionsCollection: BusDefinitionsCollection) extends Actor with StrictLogging {
  val name: String = self.path.name

  def receive = active(None, List.empty, List.empty)

  def active(route: Option[BusRoute], stopArrivalRecords: List[StopArrivalRecord], busStopDefinitionList: List[BusStop]): Receive = {
    case vsl: ValidatedSourceLine =>
      logger.info(s"actor $name received sourceLine: $vsl")
      assert(vsl.vehicleID == name)
      if (route.isEmpty) {
        val stopDefinitionList = busDefinitionsCollection.getBusRouteDefinitionsFromDB(vsl.busRoute)
        val newStopArrivalRecord = StopArrivalRecord(vsl.busStop, vsl.arrival_TimeStamp)
        context.become(active(Some(vsl.busRoute), List(newStopArrivalRecord), stopDefinitionList))
      }
      else if (route.get != vsl.busRoute) {
        //TODO Handle change of route/direction - persist?
        val stopDefinitionList = busDefinitionsCollection.getBusRouteDefinitionsFromDB(vsl.busRoute)
        val newStopArrivalRecord = StopArrivalRecord(vsl.busStop, vsl.arrival_TimeStamp)
        context.become(active(Some(vsl.busRoute), List(newStopArrivalRecord), stopDefinitionList))
      }
      else {
        if (stopArrivalRecords.head.busStop == vsl.busStop) {
          //Replace the head if stop is the same (but  time different)
          val replacementStopArrivalRecord = StopArrivalRecord(vsl.busStop, vsl.arrival_TimeStamp)
          context.become(active(route, replacementStopArrivalRecord :: stopArrivalRecords.tail, busStopDefinitionList))
        } else if (busStopDefinitionList.indexOf(vsl.busStop) > busStopDefinitionList.indexOf(stopArrivalRecords.head.busStop)) {
          if (busStopDefinitionList.indexOf(vsl.busStop) == busStopDefinitionList.indexOf(stopArrivalRecords.head.busStop) + 1) {
            if (vsl.busStop == busStopDefinitionList.last) {
              //TODO persist here
              logger.info(s"End of route reached for route: ${route.get} \n" +
                s"Stop arrival records: $stopArrivalRecords")
            } else {
              val newStopArrivalRecord = StopArrivalRecord(vsl.busStop, vsl.arrival_TimeStamp)
              context.become(active(Some(vsl.busRoute), newStopArrivalRecord :: stopArrivalRecords, busStopDefinitionList))
            }
          } else {
            logger.info(s"Line received more than one stop ahead of previously recorded line \n " +
              s"Bus Route: ${route.get} \n" +
              s"StopArrival Records: $stopArrivalRecords \n" +
              s"New Line Received: $vsl")
            val newStopArrivalRecord = StopArrivalRecord(vsl.busStop, vsl.arrival_TimeStamp)
            // Remove previous list and start afresh...
            context.become(active(Some(vsl.busRoute), List(newStopArrivalRecord), busStopDefinitionList))
          }
        } else {
          // Do nothing as stop received is behind last stop processed
        }
      }
  }
}
