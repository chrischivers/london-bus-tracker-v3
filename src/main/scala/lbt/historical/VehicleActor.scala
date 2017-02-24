package lbt.historical

import akka.actor.Actor
import com.typesafe.scalalogging.StrictLogging
import lbt.comon.{BusRoute, BusStop}
import lbt.database.definitions.BusDefinitionsCollection
import lbt.database.historical.{HistoricalRecordsCollection, HistoricalRecordsDBController}

import scalaz.Scalaz._
import scalaz._

case class VehicleRecordedDataToPersist(vehicleID: String, busRoute: BusRoute, stopArrivalRecords: List[(BusStop, Long)])

class VehicleActor(busDefinitionsCollection: BusDefinitionsCollection, historicalRecordsCollection: HistoricalRecordsCollection) extends Actor with StrictLogging {
  //TODO consider call up to parent
  val name: String = self.path.name
  type StringValidation[T] = ValidationNel[String, T]

  def receive = active(None, Map.empty, List.empty, System.currentTimeMillis())

  def active(route: Option[BusRoute],
             stopArrivalRecords: Map[BusStop, Long],
             busStopDefinitionList: List[BusStop],
             lastLineReceivedTime: Long): Receive = {

    case vsl: ValidatedSourceLine =>
      logger.info(s"actor $name received sourceLine: $vsl")
      assert(vsl.vehicleID == name)
      val timeReceived = System.currentTimeMillis()

      if (route.isEmpty) {
        val stopDefinitionList = busDefinitionsCollection.getBusRouteDefinitionsFromDB(vsl.busRoute)
        context.become(active(Some(vsl.busRoute), stopArrivalRecords + (vsl.busStop -> vsl.arrival_TimeStamp), stopDefinitionList, timeReceived))
      }
      else if (route.get != vsl.busRoute) {
        println("Change of route. Attempting to persist...")
        validateBeforePersist(route.get, stopArrivalRecords, busStopDefinitionList) match {
          case Success(completeList) => historicalRecordsCollection.insertHistoricalRecordIntoDB(VehicleRecordedDataToPersist(name, route.get, completeList))
          case Failure(e) => logger.info(s"Failed validation before persisting. Stop Arrival Records. Error: $e. \n Stop Arrival Records $stopArrivalRecords.")
        }
        val stopDefinitionList = busDefinitionsCollection.getBusRouteDefinitionsFromDB(vsl.busRoute)
        context.become(active(Some(vsl.busRoute), Map(vsl.busStop -> vsl.arrival_TimeStamp), stopDefinitionList, timeReceived))
      }
      else {
        val stopArrivalRecordsWithLastStop = stopArrivalRecords + (vsl.busStop -> vsl.arrival_TimeStamp)
        context.become(active(Some(vsl.busRoute), stopArrivalRecordsWithLastStop, busStopDefinitionList, timeReceived))
        if (vsl.busStop == busStopDefinitionList.last) {
          println("End of route reached. Attempting to persist...")
          validateBeforePersist(route.get, stopArrivalRecordsWithLastStop, busStopDefinitionList) match {
            case Success(completeList) => historicalRecordsCollection.insertHistoricalRecordIntoDB(VehicleRecordedDataToPersist(name, route.get, completeList))
            case Failure(e) => logger.info(s"Failed validation before persisting. Stop Arrival Records. Error: $e. \n Stop Arrival Records: $stopArrivalRecords.")
          }
        }
      }
    case GetArrivalRecords(_) => {
      sender ! stopArrivalRecords
    }
  }

  def validateBeforePersist(route: BusRoute, stopArrivalRecords: Map[BusStop, Long], busStopDefinitionList: List[BusStop]): StringValidation[List[(BusStop, Long)]] = {

    val orderedStopsList: List[(BusStop, Option[Long])] = busStopDefinitionList.map(stop => (stop, stopArrivalRecords.get(stop)))

    def noGapsInSequence: StringValidation[Unit] = {
      val orderedWithIndex = orderedStopsList.zipWithIndex
      val orderedWithIndexExisting = orderedWithIndex.filter(elem => elem._1._2.isDefined)
      if (orderedWithIndexExisting.size == orderedWithIndexExisting.last._2 - orderedWithIndexExisting.head._2 + 1) ().successNel
      else  s"Gaps encountered in the sequence received (excluding those at beginning and/or end. StopList: $orderedStopsList".failureNel
    }

    def stopArrivalTimesAreIncremental: StringValidation[Unit] = {
      if (isSorted(orderedStopsList.filter(stop => stop._2.isDefined).map(stop => stop._2.get), (a: Long, b: Long) => a < b)) ().successNel
      else s"Arrival times in list are not in time order. Stop list: $orderedStopsList".failureNel
    }

    def isSorted[A](as: Seq[A], ordered: (A, A) => Boolean): Boolean = {

      def helper(pos: Int): Boolean = {
        if (pos == as.length - 1) true
        else if (ordered(as(pos), as(pos + 1))) helper(pos + 1)
        else false
      }
      helper(0)
    }

    (noGapsInSequence
      |@| stopArrivalTimesAreIncremental).tupled
      .map(_ => orderedStopsList
        .filter(stop => stop._2.isDefined)
        .map(elem => (elem._1, elem._2.get)))
    }
}
