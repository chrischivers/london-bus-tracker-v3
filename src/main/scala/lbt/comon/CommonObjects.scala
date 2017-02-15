package lbt.comon

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
  def directionStrToDirection(directionStr: String) = {
    directionStr match {
      case "inbound" => Inbound()
      case "outbound" => Outbound()
      case e => throw new IllegalStateException(s"Unknown direction for string $directionStr"
      )
    }
  }
}

