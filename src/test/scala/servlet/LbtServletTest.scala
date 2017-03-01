package servlet

import datasource.TestDataSource
import lbt.comon.{BusRoute, BusStop}
import lbt.database.historical.{HistoricalRecordFromDb, HistoricalRecordsCollection}
import lbt.datasource.streaming.DataStreamProcessingController
import lbt.servlet.LbtServlet
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import org.scalatra.test.scalatest.ScalatraSuite
import servlet.LbtServletTestFixture
import net.liftweb.json._
import net.liftweb.json.JsonDSL._
import org.scalactic.source.Position
import org.scalatest.concurrent.ScalaFutures


class LbtServletTest extends FunSuite with ScalatraSuite with ScalaFutures with Matchers with BeforeAndAfterAll with LbtServletTestFixture {

  implicit val formats = DefaultFormats

  addServlet(new LbtServlet(testDefinitionsCollection, testHistoricalRecordsCollection), "/*")

  test("Should produce 404 for undefined paths") {
    get("/") {
      status should equal(404)
    }
    get("/test") {
      status should equal(404)
    }
  }

  test("should produce a 404 for unspecified route or direction") {
    get("/unknownRoute/unknownDirection/" + definitions(testBusRoutes.head).head.id + "/" + definitions(testBusRoutes.head)(2).id) {
      status should equal(404)
    }
  }

  test("Should produce a list of routes in the DB") {
    get("/routelist") {
      status should equal(200)
      parse(body).extract[List[BusRoute]].foreach(route =>
        testBusRoutes should contain(route)
      )
    }
  }
  test("Should produce a list of stops (in order) for a given route") {
    testBusRoutes.foreach(route =>
      get("/" + route.id + "/" + route.direction + "/stoplist") {
        status should equal(200)
        parse(body).extract[List[BusStop]] should equal(definitions(route))
      }
    )
  }

  test("Should produce a 404 for unspecified route or direction") {
    get("/unknown-route/outbound/stoplist") {
      status should equal(404)
    }
    get("/" + testBusRoutes.head.id + "/unknown-direction/stoplist") {
      status should equal(404)
    }
  }

  test("should produce a list of vehicles and their arrival times for a given route") {
    testBusRoutes.foreach(route =>
      get("/" + route.id + "/" + route.direction) {
        status should equal(200)
        parse(body).extract[List[HistoricalRecordFromDb]] should equal(testHistoricalRecordsCollection.getHistoricalRecordFromDB(route))
        println(parse(body))
        parse(body).extract[List[HistoricalRecordFromDb]].size shouldBe 1
        parse(body).extract[List[HistoricalRecordFromDb]].foreach(record =>
          record.stopRecords.size should equal(definitions(route).size)
        )
      }
    )
  }

  test("should produce a list of vehicles and their arrival times for a given fromStopID on a given route") {
    testBusRoutes.foreach(route => {
      val routeDef = definitions(route)
      val fromStopID = routeDef(routeDef.size / 3).id
      get("/" + route.id + "/" + route.direction + "?fromStopID=" + fromStopID) {
        status should equal(200)
        parse(body).extract[List[HistoricalRecordFromDb]] should equal(testHistoricalRecordsCollection.getHistoricalRecordFromDB(route, Some(fromStopID), None))
        parse(body).extract[List[HistoricalRecordFromDb]].size shouldBe 1
        parse(body).extract[List[HistoricalRecordFromDb]].foreach(record => {
          val indexOfFromStopID = routeDef.indexWhere(x => x.id == fromStopID)
          record.stopRecords.size should equal(routeDef.size - indexOfFromStopID)
        })
      }
    })
  }

  test("should produce a list of vehicles and their arrival times for a given toStopID on a given route") {
    testBusRoutes.foreach(route => {
      val routeDef = definitions(route)
      val toStopID = routeDef((routeDef.size / 3) * 2).id
      get("/" + route.id + "/" + route.direction + "?toStopID=" + toStopID) {
        status should equal(200)
        parse(body).extract[List[HistoricalRecordFromDb]] should equal(testHistoricalRecordsCollection.getHistoricalRecordFromDB(route, None, Some(toStopID)))
        parse(body).extract[List[HistoricalRecordFromDb]].size shouldBe 1
        parse(body).extract[List[HistoricalRecordFromDb]].foreach(record => {
          val indexOfToStopID = routeDef.indexWhere(x => x.id == toStopID) + 1
          record.stopRecords.size should equal(indexOfToStopID)
        })
      }
    })
  }

  test("should produce a list of vehicles and their arrival times for a given toStopID and fromStopID on a given route") {
    testBusRoutes.foreach(route => {
      val routeDef = definitions(route)
      val fromStopID = routeDef(routeDef.size / 3).id
      val toStopID = routeDef((routeDef.size / 3) * 2).id
      get("/" + route.id + "/" + route.direction + "?fromStopID=" + fromStopID + "&toStopID=" + toStopID) {
        status should equal(200)
        parse(body).extract[List[HistoricalRecordFromDb]] should equal(testHistoricalRecordsCollection.getHistoricalRecordFromDB(route, Some(fromStopID), Some(toStopID)))
        parse(body).extract[List[HistoricalRecordFromDb]].size shouldBe 1
        parse(body).extract[List[HistoricalRecordFromDb]].foreach(record => {
          val indexOfFromStopID = routeDef.indexWhere(x => x.id == fromStopID)
          val indexOfToStopID = routeDef.indexWhere(x => x.id == toStopID) + 1
          record.stopRecords.size should equal(indexOfToStopID - indexOfFromStopID)
        })
      }
    })
  }


  test("should produce a list of vehicles and their arrival times for corner cases at beginning of route") {
    testBusRoutes.foreach(route => {
      val routeDef = definitions(route)
      val fromStopID = routeDef.head.id
      val toStopID = routeDef.head.id
      get("/" + route.id + "/" + route.direction + "?fromStopID=" + fromStopID + "&toStopID=" + toStopID) {
        status should equal(200)
        parse(body).extract[List[HistoricalRecordFromDb]] should equal(testHistoricalRecordsCollection.getHistoricalRecordFromDB(route, Some(fromStopID), Some(toStopID)))
        parse(body).extract[List[HistoricalRecordFromDb]].size shouldBe 1
        parse(body).extract[List[HistoricalRecordFromDb]].foreach(record => record.stopRecords.size shouldBe 1)
      }

      get("/" + route.id + "/" + route.direction + "?toStopID=" + toStopID) {
        status should equal(200)
        parse(body).extract[List[HistoricalRecordFromDb]] should equal(testHistoricalRecordsCollection.getHistoricalRecordFromDB(route, None, Some(toStopID)))
        parse(body).extract[List[HistoricalRecordFromDb]].size shouldBe 1
        parse(body).extract[List[HistoricalRecordFromDb]].foreach(record => record.stopRecords.size shouldBe 1)
      }

      get("/" + route.id + "/" + route.direction + "?fromStopID=" + fromStopID) {
        status should equal(200)
        parse(body).extract[List[HistoricalRecordFromDb]] should equal(testHistoricalRecordsCollection.getHistoricalRecordFromDB(route, Some(fromStopID), None))
        parse(body).extract[List[HistoricalRecordFromDb]].size shouldBe 1
        parse(body).extract[List[HistoricalRecordFromDb]].foreach(record => record.stopRecords.size shouldBe routeDef.size)
      }
    })
  }

  test("should produce a list of vehicles and their arrival times for corner cases at end of route") {
    testBusRoutes.foreach(route => {
      val routeDef = definitions(route)
      val fromStopID = routeDef.last.id
      val toStopID = routeDef.last.id
      get("/" + route.id + "/" + route.direction + "?fromStopID=" + fromStopID + "&toStopID=" + toStopID) {
        status should equal(200)
        parse(body).extract[List[HistoricalRecordFromDb]] should equal(testHistoricalRecordsCollection.getHistoricalRecordFromDB(route, Some(fromStopID), Some(toStopID)))
        parse(body).extract[List[HistoricalRecordFromDb]].size shouldBe 1
        parse(body).extract[List[HistoricalRecordFromDb]].foreach(record => record.stopRecords.size shouldBe 1)
      }

      get("/" + route.id + "/" + route.direction + "?toStopID=" + toStopID) {
        status should equal(200)
        parse(body).extract[List[HistoricalRecordFromDb]] should equal(testHistoricalRecordsCollection.getHistoricalRecordFromDB(route, None, Some(toStopID)))
        parse(body).extract[List[HistoricalRecordFromDb]].size shouldBe 1
        parse(body).extract[List[HistoricalRecordFromDb]].foreach(record => record.stopRecords.size shouldBe routeDef.size)
      }

      get("/" + route.id + "/" + route.direction + "?fromStopID=" + fromStopID) {
        status should equal(200)
        parse(body).extract[List[HistoricalRecordFromDb]] should equal(testHistoricalRecordsCollection.getHistoricalRecordFromDB(route, Some(fromStopID), None))
        parse(body).extract[List[HistoricalRecordFromDb]].size shouldBe 1
        parse(body).extract[List[HistoricalRecordFromDb]].foreach(record => record.stopRecords.size shouldBe 1)
      }
    })
  }

  test("should produce a 404 for invalid from and to stop ID") {
    get("/" + testBusRoutes.head.id + "/" + testBusRoutes.head.direction + "?fromStopID=" + definitions(testBusRoutes.head).head.id + "&toStopID=unknownToStopID") {
      status should equal(404)
    }
    get("/" + testBusRoutes.head.id + "/" + testBusRoutes.head.direction + "?fromStopID=UnknownFromStopID&toStopID=" + definitions(testBusRoutes.head)(2).id) {
      status should equal(404)
    }
  }

  test("should produce a 404 where from stop ID does not precede to stop ID") {
    get("/" + testBusRoutes.head.id + "/" + testBusRoutes.head.direction + "?fromStopID=" + definitions(testBusRoutes.head)(2).id + "&toStopID=" + definitions(testBusRoutes.head).head.id) {
      status should equal(404)
    }
  }

  test("should produce a list of vehicles and their arrival times for two given stop IDs on a given route, in a given window of time") {
    testBusRoutes.foreach(route => {
      val routeDef = definitions(route)
      val fromStopID = routeDef(routeDef.size / 3).id
      val toStopID = routeDef((routeDef.size / 3) * 2).id
      val fromTime = System.currentTimeMillis() + (60000 * 3)
      val toTime = System.currentTimeMillis() + (60000 * 7)
      get("/" + route.id + "/" + route.direction + "?fromStopID=" + fromStopID + "&toStopID=" + toStopID + "&fromTime=" + fromTime + "&toTime=" + toTime) {
        status should equal(200)
        parse(body).extract[List[HistoricalRecordFromDb]] should equal(testHistoricalRecordsCollection.getHistoricalRecordFromDB(route, Some(fromStopID), Some(toStopID), Some(fromTime), Some(toTime)))
        parse(body).extract[List[HistoricalRecordFromDb]].foreach(record =>
          record.stopRecords.count(x => x.arrivalTime < fromTime && x.arrivalTime > toTime) shouldBe 0
        )
      }
    })
  }

  test("should produce a list of vehicles and their arrival times for a given window of time (no stop IDs specified)") {
    testBusRoutes.foreach(route => {
      val fromTime = System.currentTimeMillis() + (60000 * 3)
      val toTime = System.currentTimeMillis() + (60000 * 7)
      get("/" + route.id + "/" + route.direction + "?fromTime=" + fromTime + "&toTime=" + toTime) {
        status should equal(200)
        parse(body).extract[List[HistoricalRecordFromDb]] should equal(testHistoricalRecordsCollection.getHistoricalRecordFromDB(route, None, None, Some(fromTime), Some(toTime)))
        parse(body).extract[List[HistoricalRecordFromDb]].size shouldBe 1
      }
    })
  }

  test("should produce an empty list where no data is available (i.e. a time window out of bounds)") {
    testBusRoutes.foreach(route => {
      val fromTime = System.currentTimeMillis() - 100000000
      val toTime = System.currentTimeMillis() - 10000000
      get("/" + route.id + "/" + route.direction + "?fromTime=" + fromTime + "&toTime=" + toTime) {
        status should equal(200)
        parse(body).extract[List[HistoricalRecordFromDb]] should equal(testHistoricalRecordsCollection.getHistoricalRecordFromDB(route, None, None, Some(fromTime), Some(toTime)))
        parse(body).extract[List[HistoricalRecordFromDb]].size shouldBe 0
        parse(body).extract[List[HistoricalRecordFromDb]] shouldBe List.empty
      }
    })
  }

  test("should produce a 404 where fromTime does not precede toTime") {
    get("/" + testBusRoutes.head.id + "/" + testBusRoutes.head.direction + "?fromTime=" + System.currentTimeMillis() + 1 + "&toTime=" + System.currentTimeMillis()) {
      status should equal(404)
    }
  }

  test("should produce a 404 where time is not a valid Long") {
    get("/" + testBusRoutes.head.id + "/" + testBusRoutes.head.direction + "?fromTime=" + "NotALong" + "&toTime=" + System.currentTimeMillis()) {
      status should equal(404)
    }
  }

  test("should produce a list of arrival information for a given vehicle") {
    testBusRoutes.foreach(route => {
      val vehicleReg = messageProcessor.lastValidatedMessage.map(_.vehicleID).get
      get("/" + route.id + "/" + route.direction + "?vehicleID=" + vehicleReg) {
        status should equal(200)
        parse(body).extract[List[HistoricalRecordFromDb]] should equal(testHistoricalRecordsCollection.getHistoricalRecordFromDB(route, None, None, None, None, Some(vehicleReg)))
      }
    })
  }

  protected override def afterAll(): Unit = {
    actorSystem.terminate().futureValue
    testDefinitionsCollection.db.dropDatabase
    testHistoricalRecordsCollection.db.dropDatabase
    super.afterAll()
  }
}
