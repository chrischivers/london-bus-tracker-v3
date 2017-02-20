package lbt.comon

case class Start()
case class Stop()

trait Direction

case class Inbound() extends Direction {
  override def toString: String = "inbound"
}

case class Outbound() extends Direction {
  override def toString: String = "outbound"
}

case class BusStop(id: String, name: String, longitude: Double, latitude: Double)

case class BusRoute(id: String, direction: Direction)

case class Bus(id: String, route: BusRoute)

object Commons {

  type BusRouteDefinitions = Map[BusRoute, List[BusStop]]

  def toDirection(directionStr: String) = {
    directionStr match {
      case "inbound" => Inbound()
      case "outbound" => Outbound()
      case _ => throw new IllegalStateException(s"Unknown direction for string $directionStr"
      )
    }
  }

  def toDirection(directionInt: Int) = {
    directionInt match {
      case 1 => Outbound()
      case 2 => Inbound()
      case _ => throw new IllegalStateException(s"Unknown direction for string $directionInt"
      )
    }
  }
}

