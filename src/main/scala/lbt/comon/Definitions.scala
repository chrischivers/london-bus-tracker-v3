package lbt.comon

case class Direction()

case class Inbound() extends Direction

case class Outbound() extends Direction

case class BusStop(id: String, name: String, longitude: Long, latitude: Long)

case class BusRoute(id: String, direction: Direction, definition: List[BusStop])

case class Bus(id: String, route: BusRoute)
