package lbt.servlet

import akka.actor.Kill
import lbt.TransmittedBusRouteWithTowards
import lbt.comon.{BusRoute, BusStop}
import net.liftweb.json._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, Matchers}
import org.scalatra.test.scalatest.ScalatraFunSuite

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Random

class LbtServletGeneralTest extends ScalatraFunSuite with ScalaFutures with Matchers with BeforeAndAfterAll with Eventually with LbtServletTestFixture {

  implicit val formats = DefaultFormats
  implicit val ec = ExecutionContext.Implicits.global
  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(30 seconds),
    interval = scaled(1 second))

  addServlet(new LbtServlet(testDefinitionsTable, testHistoricalTable, dataStreamProcessor, historicalSourceLineProcessor, vehicleActorSupervisor, historicalRecordsFetcher), "/*")

  Thread.sleep(10000)

  test("Should produce 404 for undefined paths") {
    get("/") {
      status should equal(404)
    }
    get("/test") {
      status should equal(404)
    }
  }

  test("should start the lbt.servlet") {
    get("/streamstart") {
      status should equal(200)
      eventually {
        dataStreamProcessor.numberLinesProcessed.futureValue should be > 0L
      }
    }
  }

  test("should stop the lbt.servlet") {
    get("/streamstart") {}
    Thread.sleep(2000)
    get("/streamstop") {
      status should equal(200)
      Thread.sleep(2000)
      val numberProcessed  = dataStreamProcessor.numberLinesProcessed.futureValue
      Thread.sleep(2000)
      numberProcessed shouldEqual dataStreamProcessor.numberLinesProcessed.futureValue
    }
  }

  test("should produce a 404 for unspecified route or direction") {
    get("/unknownRoute/unknownDirection/" + definitions(testBusRoutes.head).head.stopID + "/" + definitions(testBusRoutes.head)(2).stopID) {
      status should equal(404)
    }
  }

  test("Should produce a list of routes in the DB") {
    get("/routelist") {
      status should equal(200)
      parse(body).extract[List[BusRoute]].foreach(route =>
        testBusRoutes :+ BusRoute("521", "inbound") should contain(route)
      )
    }
  }

  test("Should produce a list of routes in the DB with their corresponding 'towards' stop") {
    get("/routelist") {
      status should equal(200)
      val parsedResponse = parse(body).extract[List[TransmittedBusRouteWithTowards]]
      testBusRoutes.foreach(route => {
        val routeFromResponse = parsedResponse.find(x => x.name == route.name && x.direction == route.direction).get
        definitions(route).last.stopName shouldBe routeFromResponse.towards
      })
    }
  }

  test("Should produce a list of stops (in order) for a given route") {
    testBusRoutes.foreach(route =>
      get("/stoplist/" + route.name + "/" + route.direction) {
        status should equal(200)
        parse(body).extract[List[BusStop]] should equal(definitions(route))
      }
    )
  }

  test("Should produce a 404 for unspecified route or direction") {
    get("/stoplist/unknown-route/outbound") {
      status should equal(400)
    }
    get("/stoplist/" + testBusRoutes.head.name + "/unknown-direction") {
      status should equal(400)
    }
  }

  test("Should get error count") {
    get("/errorcount") {
      status should equal(200)
      body should include(testBusRoutes.head.name + " - " + testBusRoutes.head.direction)
    }
  }

  test("Should get stop details for a given stopID") {
    val stop = definitions.head._2(Random.nextInt(definitions.head._2.size))
    get("/stopdetails/" + stop.stopID) {
      status should equal(200)
      parse(body).extract[BusStop] shouldEqual stop
    }
  }

  protected override def afterAll(): Unit = {
    actorSystem.terminate().futureValue
    testDefinitionsTable.deleteTable
    testHistoricalTable.deleteTable
    dataStreamProcessor.processorControllerActor ! Kill
    Thread.sleep(1000)
    super.afterAll()
  }
}
