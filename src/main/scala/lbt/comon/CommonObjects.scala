package lbt.comon

case class Start()
case class Stop()


case class BusStop(id: String, name: String, longitude: Double, latitude: Double)

case class BusRoute(id: String, direction: String)

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

