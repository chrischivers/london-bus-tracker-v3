package lbt.comon

trait Direction

case class Inbound() extends Direction

case class Outbound() extends Direction

case class BusStop(id: String, name: String, longitude: BigDecimal, latitude: BigDecimal)

case class BusRoute(id: String, direction: Direction)

case class Bus(id: String, route: BusRoute)
