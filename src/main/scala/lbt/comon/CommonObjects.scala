package lbt.comon

case class Start()
case class Stop()

case class StopID(value: String)

case class SeqNo(value: Int)

case class StopName(value: String)

case class BusStop(id: StopID, name: StopName, longitude: Double, latitude: Double)

case class RouteID(value: String)

case class Direction(value: String)

case class BusRoute(id: RouteID, direction: Direction)

case class VehicleReg(value: String)

object Commons {

  type BusRouteDefinitions = Map[BusRoute, List[BusStop]]

  def toDirection(directionInt: Int): Direction = {
    directionInt match {
      case 1 => Direction("outbound")
      case 2 => Direction("inbound")
      case _ => throw new IllegalStateException(s"Unknown direction for string $directionInt"
      )
    }
  }
}

