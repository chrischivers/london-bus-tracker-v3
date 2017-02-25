package lbt.servlet

import lbt.database.definitions.BusDefinitionsCollection
import org.scalatra.ScalatraServlet
import net.liftweb.json._
import net.liftweb.json.JsonDSL._


class LbtServlet(busDefinitionsCollection: BusDefinitionsCollection) extends ScalatraServlet {

  implicit val formats = DefaultFormats

  get("/routelist") {
    val x = compactRender(busDefinitionsCollection.getBusRouteDefinitionsFromDB.keys.toList.map(key =>
      ("id" -> key.id) ~ ("direction" -> key.direction.toString)))
      println (x)
      x
  }

}
