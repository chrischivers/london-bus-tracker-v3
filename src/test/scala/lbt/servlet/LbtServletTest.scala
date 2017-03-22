package lbt.servlet

import akka.actor.Kill
import lbt.comon.{BusRoute, BusStop}
import lbt.database.historical.{ArrivalRecord, HistoricalRecordFromDb}
import net.liftweb.json._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FunSuite, FunSuiteLike, Matchers}
import org.scalatra.test.scalatest.{ScalatraFunSuite, ScalatraSuite}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

case class TransmittedBusRouteWithTowards(name: String, direction: String, towards: String)
case class TransmittedIncomingHistoricalRecord(busRoute: BusRoute, vehicleID: String, stopRecords: List[TransmittedIncomingVehicleStopRecord])
case class TransmittedIncomingVehicleStopRecord(seqNo: Int, busStop: BusStop, arrivalTime: Long)


class LbtServletTest extends ScalatraFunSuite with ScalaFutures with Matchers with BeforeAndAfterAll with Eventually with LbtServletTestFixture {

  implicit val formats = DefaultFormats
  implicit val ec = ExecutionContext.Implicits.global
  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(30 seconds),
    interval = scaled(1 second))

  addServlet(new LbtServlet(testDefinitionsTable, testHistoricalTable, dataStreamProcessor, historicalSourceLineProcessor), "/*")



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
        testBusRoutes should contain(route)
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
      status should equal(404)
    }
    get("/stoplist/" + testBusRoutes.head.name + "/unknown-direction") {
      status should equal(404)
    }
  }

  test("should produce a list of vehicles and their arrival times for a given route") {
    testBusRoutes.foreach(route =>
      get("/busroute/" + route.name + "/" + route.direction) {
        status should equal(200)
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(testHistoricalTable.getHistoricalRecordFromDbByBusRoute(route))
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          val stopsForRoute = definitions(route)
          record.stopRecords.map(stopRecs => stopRecs.busStop) shouldEqual stopsForRoute
          record.vehicleID shouldEqual vehicleReg
        })
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 1
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record =>
          record.stopRecords.size should equal(definitions(route).size)
        )
      }
    )
  }

  test("should produce a list of vehicles and their arrival times for a given fromStopID on a given route") {
    testBusRoutes.foreach(route => {
      val routeDef = definitions(route)
      val fromStopID = routeDef(routeDef.size / 3).stopID
      get("/busroute/" + route.name + "/" + route.direction + "?fromStopID=" + fromStopID) {
        status should equal(200)
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(testHistoricalTable.getHistoricalRecordFromDbByBusRoute(route, Some(fromStopID), None))
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 1
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          val indexOfFromStopID = routeDef.indexWhere(x => x.stopID == fromStopID)
          record.stopRecords.size should equal(routeDef.size - indexOfFromStopID)
          record.vehicleID shouldEqual vehicleReg
        })
      }
    })
  }

  test("should produce a list of vehicles and their arrival times for a given toStopID on a given route") {
    testBusRoutes.foreach(route => {
      val routeDef = definitions(route)
      val toStopID = routeDef((routeDef.size / 3) * 2).stopID
      get("/busroute/" + route.name + "/" + route.direction + "?toStopID=" + toStopID) {
        status should equal(200)
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(testHistoricalTable.getHistoricalRecordFromDbByBusRoute(route, None, Some(toStopID)))
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 1
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          val indexOfToStopID = routeDef.indexWhere(x => x.stopID == toStopID) + 1
          record.stopRecords.size should equal(indexOfToStopID)
          record.vehicleID shouldEqual vehicleReg
        })
      }
    })
  }

  test("should produce a list of vehicles and their arrival times for a given toStopID and fromStopID on a given route") {
    testBusRoutes.foreach(route => {
      val routeDef = definitions(route)
      val fromStopID = routeDef(routeDef.size / 3).stopID
      val toStopID = routeDef((routeDef.size / 3) * 2).stopID
      get("/busroute/" + route.name + "/" + route.direction + "?fromStopID=" + fromStopID + "&toStopID=" + toStopID) {
        status should equal(200)
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(testHistoricalTable.getHistoricalRecordFromDbByBusRoute(route, Some(fromStopID), Some(toStopID)))
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 1
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          val indexOfFromStopID = routeDef.indexWhere(x => x.stopID == fromStopID)
          val indexOfToStopID = routeDef.indexWhere(x => x.stopID == toStopID) + 1
          record.stopRecords.size should equal(indexOfToStopID - indexOfFromStopID)
          record.vehicleID shouldEqual vehicleReg
        })
      }
    })
  }


  test("should produce a list of vehicles and their arrival times for corner cases at beginning of route") {
    testBusRoutes.foreach(route => {
      val routeDef = definitions(route)
      val fromStopID = routeDef.head.stopID
      val toStopID = routeDef.head.stopID
      get("/busroute/" + route.name + "/" + route.direction + "?fromStopID=" + fromStopID + "&toStopID=" + toStopID) {
        status should equal(200)
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(testHistoricalTable.getHistoricalRecordFromDbByBusRoute(route, Some(fromStopID), Some(toStopID)))
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 1
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          record.stopRecords.head.busStop.stopID shouldEqual routeDef.head.stopID
          record.stopRecords.size shouldBe 1
          record.vehicleID shouldEqual vehicleReg
        })
      }

      get("/busroute/" + route.name + "/" + route.direction + "?toStopID=" + toStopID) {
        status should equal(200)
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(testHistoricalTable.getHistoricalRecordFromDbByBusRoute(route, None, Some(toStopID)))
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 1
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          record.stopRecords.head.busStop.stopID shouldEqual routeDef.head.stopID
          record.stopRecords.size shouldBe 1
          record.vehicleID shouldEqual vehicleReg
        })
      }

      get("/busroute/" + route.name + "/" + route.direction + "?fromStopID=" + fromStopID) {
        status should equal(200)
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(testHistoricalTable.getHistoricalRecordFromDbByBusRoute(route, Some(fromStopID), None))
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 1
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          record.stopRecords.head.busStop.stopID shouldEqual routeDef.head.stopID
          record.stopRecords.size shouldBe routeDef.size
          record.vehicleID shouldEqual vehicleReg
        })
      }
    })
  }

  test("should produce a list of vehicles and their arrival times for corner cases at end of route") {
    testBusRoutes.foreach(route => {
      val routeDef = definitions(route)
      val fromStopID = routeDef.last.stopID
      val toStopID = routeDef.last.stopID
      get("/busroute/" + route.name + "/" + route.direction + "?fromStopID=" + fromStopID + "&toStopID=" + toStopID) {
        status should equal(200)
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(testHistoricalTable.getHistoricalRecordFromDbByBusRoute(route, Some(fromStopID), Some(toStopID)))
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 1
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          record.stopRecords.head.busStop.stopID shouldEqual routeDef.last.stopID
          record.stopRecords.size shouldBe 1
          record.vehicleID shouldEqual vehicleReg
        })
      }

      get("/busroute/" + route.name + "/" + route.direction + "?toStopID=" + toStopID) {
        status should equal(200)
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(testHistoricalTable.getHistoricalRecordFromDbByBusRoute(route, None, Some(toStopID)))
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 1
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          record.stopRecords.map(records => records.busStop) shouldEqual routeDef
          record.stopRecords.size shouldBe routeDef.size
          record.vehicleID shouldEqual vehicleReg
        })
      }

      get("/busroute/" + route.name + "/" + route.direction + "?fromStopID=" + fromStopID) {
        status should equal(200)
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(testHistoricalTable.getHistoricalRecordFromDbByBusRoute(route, Some(fromStopID), None))
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 1
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          record.stopRecords.head.busStop.stopID shouldEqual routeDef.last.stopID
          record.stopRecords.size shouldBe 1
          record.vehicleID shouldEqual vehicleReg
        })
      }
    })
  }

  test("should produce a 404 for invalid from and to stop ID") {
    get("/busroute/" + testBusRoutes.head.name + "/" + testBusRoutes.head.direction + "?fromStopID=" + definitions(testBusRoutes.head).head.stopID + "&toStopID=unknownToStopID") {
      status should equal(404)
    }
    get("/busroute/" + testBusRoutes.head.name + "/" + testBusRoutes.head.direction + "?fromStopID=UnknownFromStopID&toStopID=" + definitions(testBusRoutes.head)(2).stopID) {
      status should equal(404)
    }
  }

  test("should produce a 404 where from stop ID does not precede to stop ID") {
    get("/busroute/" + testBusRoutes.head.name + "/" + testBusRoutes.head.direction + "?fromStopID=" + definitions(testBusRoutes.head)(2).stopID + "&toStopID=" + definitions(testBusRoutes.head).head.stopID) {
      status should equal(404)
    }
  }

  test("should produce a list of vehicles and their arrival times for two given stop IDs on a given route, in a given window of time") {
    testBusRoutes.foreach(route => {
      val routeDef = definitions(route)
      val fromStopID = routeDef(routeDef.size / 3).stopID
      val toStopID = routeDef((routeDef.size / 3) * 2).stopID
      val fromTime = System.currentTimeMillis() + (60000 * 3)
      val toTime = System.currentTimeMillis() + (60000 * 7)
      get("/busroute/" + route.name + "/" + route.direction + "?fromStopID=" + fromStopID + "&toStopID=" + toStopID + "&fromTime=" + fromTime + "&toTime=" + toTime) {
        status should equal(200)
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(testHistoricalTable.getHistoricalRecordFromDbByBusRoute(route, Some(fromStopID), Some(toStopID), Some(fromTime), Some(toTime)))
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record =>
          record.stopRecords.count(x => x.arrivalTime < fromTime && x.arrivalTime > toTime) shouldBe 0
        )
      }
    })
  }

  test("should produce a list of vehicles and their arrival times for a given window of time (i.e. no stop IDs specified)") {
    testBusRoutes.foreach(route => {
      val fromTime = System.currentTimeMillis() + (60000 * 3)
      val toTime = System.currentTimeMillis() + (60000 * 7)
      get("/busroute/" + route.name + "/" + route.direction + "?fromTime=" + fromTime + "&toTime=" + toTime) {
        status should equal(200)
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(testHistoricalTable.getHistoricalRecordFromDbByBusRoute(route, None, None, Some(fromTime), Some(toTime)))
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 1
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          val stopsForRoute = definitions(route)
          record.vehicleID shouldEqual vehicleReg
          record.stopRecords.exists(rec => rec.arrivalTime < fromTime) shouldBe false
          record.stopRecords.exists(rec => rec.arrivalTime > toTime) shouldBe false
        })
      }
    })
  }

  test("should produce an empty list where no data is available (i.e. a time window out of bounds)") {
    testBusRoutes.foreach(route => {
      val fromTime = System.currentTimeMillis() - 100000000
      val toTime = System.currentTimeMillis() - 10000000
      get("/busroute/" + route.name + "/" + route.direction + "?fromTime=" + fromTime + "&toTime=" + toTime) {
        status should equal(200)
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(testHistoricalTable.getHistoricalRecordFromDbByBusRoute(route, None, None, Some(fromTime), Some(toTime)))
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 0
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]] shouldBe List.empty
      }
    })
  }

  test("should produce a 404 where fromTime does not precede toTime") {
    get("/busroute/" + testBusRoutes.head.name + "/" + testBusRoutes.head.direction + "?fromTime=" + System.currentTimeMillis() + 1 + "&toTime=" + System.currentTimeMillis()) {
      status should equal(404)
    }
  }

  test("should produce a 404 where time is not a valid Long") {
    get("/busroute/" + testBusRoutes.head.name + "/" + testBusRoutes.head.direction + "?fromTime=" + "NotALong" + "&toTime=" + System.currentTimeMillis()) {
      status should equal(404)
    }
  }

  test("should produce a list of arrival information for a given vehicle") {
    testBusRoutes.foreach(route => {
      get("/busroute/" + route.name + "/" + route.direction + "?vehicleID=" + vehicleReg) {
        status should equal(200)
          toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(testHistoricalTable.getHistoricalRecordFromDbByBusRoute(route, None, None, None, None, Some(vehicleReg)))
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          val stopsForRoute = definitions(route)
          record.stopRecords.map(stopRecs => stopRecs.busStop) shouldEqual stopsForRoute
          record.vehicleID shouldEqual vehicleReg
        })
      }
    })
  }

  def toDbRecord(transmittedIncomingHistoricalRecordList: List[TransmittedIncomingHistoricalRecord]): List[HistoricalRecordFromDb] = {
    transmittedIncomingHistoricalRecordList
      .map(x => HistoricalRecordFromDb(x.busRoute, x.vehicleID, x.stopRecords
        map(y => ArrivalRecord(y.seqNo, y.busStop.stopID, y.arrivalTime))))
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
