import lbt.comon.BusRoute
import lbt.servlet.LbtServlet
import org.scalatest.{FunSuite, Matchers}
import org.scalatra.test.scalatest.ScalatraSuite
import servlet.LbtServletTestFixture
import net.liftweb.json._
import net.liftweb.json.JsonDSL._


class LbtServletTest extends FunSuite with ScalatraSuite with Matchers with LbtServletTestFixture {

  implicit val formats = DefaultFormats

  addServlet(new LbtServlet(testDefinitionsCollection), "/*")

  test("Data stream should be opened and return with next value") {
    get("/") {
      status should equal (404)
    }
  }

  test("Should produce a list of routes in the DB") {
    get("/routelist") {
      status should equal (200)
      parse(body).extract[List[BusRoute]] should equal(testBusRoutes)
    }
  }
}
