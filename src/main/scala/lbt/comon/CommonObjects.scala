package lbt.comon

import org.joda.time.DateTime

case class Start()
case class Stop()


case class BusStop(stopID: String, stopName: String, latitude: Double, longitude: Double)

case class BusRoute(name: String, direction: String)

object Commons {

  type BusRouteDefinitions = Map[BusRoute, List[BusStop]]
  type BusStopDefinitions = Map[String, BusStop]

  def toDirection(directionInt: Int): String = {
    directionInt match {
      case 1 => "outbound"
      case 2 => "inbound"
      case _ => throw new IllegalStateException(s"Unknown direction for string $directionInt"
      )
    }
  }

  def getSecondsOfWeek(journeyStartTime: Long): Int = {
    val dateTime = new DateTime(journeyStartTime)
    ((dateTime.getDayOfWeek - 1) * 86400) + dateTime.getSecondOfDay
  }
}

