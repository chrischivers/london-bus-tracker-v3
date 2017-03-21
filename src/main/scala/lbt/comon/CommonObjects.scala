package lbt.comon

import org.joda.time.DateTime

case class Start()
case class Stop()


case class BusStop(stopID: String, stopName: String, longitude: Double, latitude: Double)

case class BusRoute(name: String, direction: String)

object Commons {

  type BusRouteDefinitions = Map[BusRoute, List[BusStop]]

  def toDirection(directionInt: Int): String = {
    directionInt match {
      case 1 => "outbound"
      case 2 => "inbound"
      case _ => throw new IllegalStateException(s"Unknown direction for string $directionInt"
      )
    }
  }
}

