package lbt.historical

import akka.actor.Actor
import com.typesafe.scalalogging.StrictLogging
import lbt.comon.{BusRoute, BusStop}
import lbt.database.definitions.BusDefinitionsCollection

import scala.collection.immutable.ListMap
import scalaz.Scalaz._
import scalaz._

class VehicleActor(busDefinitionsCollection: BusDefinitionsCollection) extends Actor with StrictLogging {
  val name: String = self.path.name
  type StringValidation[T] = ValidationNel[String, T]

  def receive = active(None, Map.empty, List.empty)

  def active(route: Option[BusRoute], stopArrivalRecords: Map[BusStop, Long], busStopDefinitionList: List[BusStop]): Receive = {
    case vsl: ValidatedSourceLine =>
      logger.info(s"actor $name received sourceLine: $vsl")
      assert(vsl.vehicleID == name)

      if (route.isEmpty) {
        val stopDefinitionList = busDefinitionsCollection.getBusRouteDefinitionsFromDB(vsl.busRoute)
        context.become(active(Some(vsl.busRoute), stopArrivalRecords + (vsl.busStop -> vsl.arrival_TimeStamp), stopDefinitionList))
      }
      else if (route.get != vsl.busRoute) {
        //TODO Handle change of route/direction - persist?
        println("Route Change. Persisting: \n" +
          validateBeforePersist(route.get, stopArrivalRecords, busStopDefinitionList))
        val stopDefinitionList = busDefinitionsCollection.getBusRouteDefinitionsFromDB(vsl.busRoute)
        //TODO handle setting up of new listmap
        context.become(active(Some(vsl.busRoute), Map(vsl.busStop -> vsl.arrival_TimeStamp), stopDefinitionList))
      }
      else {
          context.become(active(Some(vsl.busRoute), stopArrivalRecords + (vsl.busStop -> vsl.arrival_TimeStamp), busStopDefinitionList))
        if (vsl.busStop == busStopDefinitionList.last) {
          println("End of route reached. Persisting: \n" +
            validateBeforePersist(route.get, stopArrivalRecords, busStopDefinitionList))
        }
      }
    case GetArrivalRecords(_) => {
      sender ! stopArrivalRecords
    }
  }

  def validateBeforePersist(route: BusRoute, stopArrivalRecords: Map[BusStop, Long], busStopDefinitionList: List[BusStop]): StringValidation[List[(BusStop, Long)]] = {

    val orderedStopsList: List[(BusStop, Option[Long])] = busStopDefinitionList.map(stop => (stop, stopArrivalRecords.get(stop)))

    def allStopsHaveTimesRecorded: StringValidation[Unit] = {
      if (orderedStopsList.exists(stop => stop._2.isEmpty)) "Arrival times have not been received for all stops in list".failureNel
      else ().successNel
      //TODO handle gaps at start?
    }

    def stopArrivalTimesAreIncremental: StringValidation[Unit] = {
      if (isSorted(orderedStopsList.filter(stop => stop._2.isDefined).map(stop => stop._2.get), (a: Long, b: Long) => a < b)) ().successNel
      else "Arrival times in list are not in time order".failureNel
    }

    def isSorted[A](as: Seq[A], ordered: (A, A) => Boolean): Boolean = {

      def helper(pos: Int): Boolean = {
        if (pos == as.length - 1) true
        else if (ordered(as(pos), as(pos + 1))) helper(pos + 1)
        else false
      }
      helper(0)
    }

    (allStopsHaveTimesRecorded
      |@| stopArrivalTimesAreIncremental).tupled.map(_ => orderedStopsList.map(elem => (elem._1, elem._2.get)))
    }
}
