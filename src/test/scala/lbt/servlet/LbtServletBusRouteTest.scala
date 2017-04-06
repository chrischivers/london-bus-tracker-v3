package lbt.servlet

import akka.actor.Kill
import lbt.TransmittedIncomingHistoricalRecord
import lbt.comon.{BusRoute, BusStop}
import lbt.database.historical.{ArrivalRecord, HistoricalJourneyRecordFromDb, Journey}
import lbt.datasource.streaming.SourceLineValidator
import net.liftweb.json._
import org.joda.time.DateTime
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FunSuite, FunSuiteLike, Matchers}
import org.scalatra.test.scalatest.{ScalatraFunSuite, ScalatraSuite}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.util.Random

class LbtServletBusRouteTest extends ScalatraFunSuite with ScalaFutures with Matchers with BeforeAndAfterAll with Eventually with LbtServletTestFixture {

  implicit val formats = DefaultFormats
  implicit val ec = ExecutionContext.Implicits.global
  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(30 seconds),
    interval = scaled(1 second))

  addServlet(new LbtServlet(testDefinitionsTable, testHistoricalTable, dataStreamProcessor, historicalSourceLineProcessor, vehicleActorSupervisor, historicalRecordsFetcher), "/*")

  Thread.sleep(10000)

  test("should produce a list of vehicles and their arrival times for a given route") {
    testBusRoutes.foreach(route =>
      get("/busroute/" + route.name + "/" + route.direction) {
        status should equal(200)
        println("BODY: " + body)
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(historicalRecordsFetcher.getsHistoricalRecordsByBusRoute(route).futureValue)
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          val stopsForRoute = definitions(route)
          record.stopRecords.map(stopRecs => stopRecs.busStop) shouldEqual stopsForRoute
          record.journey.vehicleReg shouldEqual vehicleReg
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
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(historicalRecordsFetcher.getsHistoricalRecordsByBusRoute(route, Some(fromStopID), None).futureValue)
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 1
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          val indexOfFromStopID = routeDef.indexWhere(x => x.stopID == fromStopID)
          record.stopRecords.size should equal(routeDef.size - indexOfFromStopID)
          record.journey.vehicleReg shouldEqual vehicleReg
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
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(historicalRecordsFetcher.getsHistoricalRecordsByBusRoute(route, None, Some(toStopID)).futureValue)
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 1
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          val indexOfToStopID = routeDef.indexWhere(x => x.stopID == toStopID) + 1
          record.stopRecords.size should equal(indexOfToStopID)
          record.journey.vehicleReg shouldEqual vehicleReg
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
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(historicalRecordsFetcher.getsHistoricalRecordsByBusRoute(route, Some(fromStopID), Some(toStopID)).futureValue)
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 1
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          val indexOfFromStopID = routeDef.indexWhere(x => x.stopID == fromStopID)
          val indexOfToStopID = routeDef.indexWhere(x => x.stopID == toStopID) + 1
          record.stopRecords.size should equal(indexOfToStopID - indexOfFromStopID)
          record.journey.vehicleReg shouldEqual vehicleReg
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
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(historicalRecordsFetcher.getsHistoricalRecordsByBusRoute(route, Some(fromStopID), Some(toStopID)).futureValue)
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 1
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          record.stopRecords.head.busStop.stopID shouldEqual routeDef.head.stopID
          record.stopRecords.size shouldBe 1
          record.journey.vehicleReg shouldEqual vehicleReg
        })
      }

      get("/busroute/" + route.name + "/" + route.direction + "?toStopID=" + toStopID) {
        status should equal(200)
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(historicalRecordsFetcher.getsHistoricalRecordsByBusRoute(route, None, Some(toStopID)).futureValue)
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 1
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          record.stopRecords.head.busStop.stopID shouldEqual routeDef.head.stopID
          record.stopRecords.size shouldBe 1
          record.journey.vehicleReg shouldEqual vehicleReg
        })
      }

      get("/busroute/" + route.name + "/" + route.direction + "?fromStopID=" + fromStopID) {
        status should equal(200)
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(historicalRecordsFetcher.getsHistoricalRecordsByBusRoute(route, Some(fromStopID), None).futureValue)
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 1
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          record.stopRecords.head.busStop.stopID shouldEqual routeDef.head.stopID
          record.stopRecords.size shouldBe routeDef.size
          record.journey.vehicleReg shouldEqual vehicleReg
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
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(historicalRecordsFetcher.getsHistoricalRecordsByBusRoute(route, Some(fromStopID), Some(toStopID)).futureValue)
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 1
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          record.stopRecords.head.busStop.stopID shouldEqual routeDef.last.stopID
          record.stopRecords.size shouldBe 1
          record.journey.vehicleReg shouldEqual vehicleReg
        })
      }

      get("/busroute/" + route.name + "/" + route.direction + "?toStopID=" + toStopID) {
        status should equal(200)
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(historicalRecordsFetcher.getsHistoricalRecordsByBusRoute(route, None, Some(toStopID)).futureValue)
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 1
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          record.stopRecords.map(records => records.busStop) shouldEqual routeDef
          record.stopRecords.size shouldBe routeDef.size
          record.journey.vehicleReg shouldEqual vehicleReg
        })
      }

      get("/busroute/" + route.name + "/" + route.direction + "?fromStopID=" + fromStopID) {
        status should equal(200)
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(historicalRecordsFetcher.getsHistoricalRecordsByBusRoute(route, Some(fromStopID), None).futureValue)
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 1
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          record.stopRecords.head.busStop.stopID shouldEqual routeDef.last.stopID
          record.stopRecords.size shouldBe 1
          record.journey.vehicleReg shouldEqual vehicleReg
        })
      }
    })
  }

  test("should produce a 400 for invalid from and to stop ID") {
    get("/busroute/" + testBusRoutes.head.name + "/" + testBusRoutes.head.direction + "?fromStopID=" + definitions(testBusRoutes.head).head.stopID + "&toStopID=unknownToStopID") {
      status should equal(400)
    }
    get("/busroute/" + testBusRoutes.head.name + "/" + testBusRoutes.head.direction + "?fromStopID=UnknownFromStopID&toStopID=" + definitions(testBusRoutes.head)(2).stopID) {
      status should equal(400)
    }
  }

  test("should produce a 400 where from stop ID does not precede to stop ID") {
    get("/busroute/" + testBusRoutes.head.name + "/" + testBusRoutes.head.direction + "?fromStopID=" + definitions(testBusRoutes.head)(2).stopID + "&toStopID=" + definitions(testBusRoutes.head).head.stopID) {
      status should equal(400)
    }
  }

  test("should produce a list of vehicles and their arrival times for two given stop IDs on a given route, in a given window of time") {
    testBusRoutes.foreach(route => {
      val routeDef = definitions(route)
      val fromStopID = routeDef(routeDef.size / 3).stopID
      val toStopID = routeDef((routeDef.size / 3) * 2).stopID
      val fromTime = now + 1000
      val toTime = now + 100000
      get("/busroute/" + route.name + "/" + route.direction + "?fromStopID=" + fromStopID + "&toStopID=" + toStopID + "&fromArrivalTimeMillis=" + fromTime + "&toArrivalTimeMillis=" + toTime) {
        status should equal(200)
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(historicalRecordsFetcher.getsHistoricalRecordsByBusRoute(route, Some(fromStopID), Some(toStopID), Some(fromTime), Some(toTime)).futureValue)
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].nonEmpty shouldBe true
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record =>
          record.stopRecords.count(x => x.arrivalTime < fromTime && x.arrivalTime > toTime) shouldBe 0
        )
      }
    })
  }

  test("should produce a list of vehicles and their arrival times for a given window of time (i.e. no stop IDs specified)") {
    testBusRoutes.foreach(route => {
      val fromTime = now + 4000
      val toTime = now + 7000
      get("/busroute/" + route.name + "/" + route.direction + "?fromArrivalTimeMillis=" + fromTime + "&toArrivalTimeMillis=" + toTime) {
        status should equal(200)
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(historicalRecordsFetcher.getsHistoricalRecordsByBusRoute(route, fromArrivalTimeMillis = Some(fromTime), toArrivalTimeMillis = Some(toTime)).futureValue)
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 1
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          record.journey.vehicleReg shouldEqual vehicleReg
          record.stopRecords.exists(rec => rec.arrivalTime < fromTime) shouldBe false
          record.stopRecords.exists(rec => rec.arrivalTime > toTime) shouldBe false
        })
      }
    })
  }

  test("should produce a list of vehicles and their arrival times for a given stopSecInWeek window") {
    testBusRoutes.foreach(route => {
      get("/busroute/" + route.name + "/" + route.direction) {

        val size = parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size
        val secOfWeek = parse(body).extract[List[TransmittedIncomingHistoricalRecord]].apply(Random.nextInt(size)).journey.startingSecondOfWeek
        get("/busroute/" + route.name + "/" + route.direction + "?fromJourneyStartSecOfWeek=" + secOfWeek + "&toJourneyStartSecOfWeek=" + secOfWeek) {
          status should equal(200)
          toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(historicalRecordsFetcher.getsHistoricalRecordsByBusRoute(route, fromJourneyStartSecOfWeek = Some(secOfWeek), toJourneyStartSecOfWeek = Some(secOfWeek)).futureValue)
          parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 1
          parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
            record.journey.vehicleReg shouldEqual vehicleReg
            record.journey.startingSecondOfWeek shouldBe >=(secOfWeek)
            record.journey.startingSecondOfWeek shouldBe <=(secOfWeek)
          })
        }
      }
    })
  }

  test("should produce an empty list where no data is available (i.e. a time window out of bounds)") {
    testBusRoutes.foreach(route => {
      val fromTime = now - 100000000
      val toTime = now - 10000000
      get("/busroute/" + route.name + "/" + route.direction + "?fromArrivalTimeMillis=" + fromTime + "&toArrivalTimeMillis=" + toTime) {
        status should equal(200)
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(historicalRecordsFetcher.getsHistoricalRecordsByBusRoute(route, fromArrivalTimeMillis = Some(fromTime), toArrivalTimeMillis = Some(toTime)).futureValue)
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 0
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]] shouldBe List.empty
      }
    })
  }

  test("should produce a 400 where fromTime does not precede toTime") {
    get("/busroute/" + testBusRoutes.head.name + "/" + testBusRoutes.head.direction + "?fromArrivalTimeMillis=" + System.currentTimeMillis() + 1 + "&toArrivalTimeMillis=" + System.currentTimeMillis()) {
      status should equal(400)
    }
  }

  test("should produce a 400 where time is not a valid Long") {
    get("/busroute/" + testBusRoutes.head.name + "/" + testBusRoutes.head.direction + "?fromArrivalTimeMillis=" + "NotALong" + "&toArrivalTimeMillis=" + System.currentTimeMillis()) {
      status should equal(400)
    }
  }

  test("should produce a list of arrival information for a given vehicle") {
    testBusRoutes.foreach(route => {
      get("/busroute/" + route.name + "/" + route.direction + "?vehicleID=" + vehicleReg) {
        status should equal(200)
          toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(historicalRecordsFetcher.getsHistoricalRecordsByBusRoute(route, vehicleReg = Some(vehicleReg)).futureValue)
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].nonEmpty shouldBe true
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          val stopsForRoute = definitions(route)
          record.stopRecords.map(stopRecs => stopRecs.busStop) shouldEqual stopsForRoute
          record.journey.vehicleReg shouldEqual vehicleReg
        })
      }
    })
  }

  test("should produce a list of vehicles and their arrival times for a given route, including times for active vehicles that haven't yet been persisted") {

    val liveBusRoute = testBusRoutes.head
    val message = SourceLineValidator("[1,\"" + definitions(liveBusRoute).head.stopID + "\",\"" + liveBusRoute.name + "\"," + directionToInt(liveBusRoute.direction) + ",\"Any Place\",\"" + vehicleReg + "Y" + "\"," + System.currentTimeMillis() + 6000 + "]").get
    historicalSourceLineProcessor.processSourceLine(message)

    get("/busroute/" + liveBusRoute.name + "/" + liveBusRoute.direction) {
      status should equal(200)
      parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe 2
      val result = parse(body).extract[List[TransmittedIncomingHistoricalRecord]]
      result.head.stopRecords.size should equal(definitions(liveBusRoute).size)
      result(1).stopRecords.size shouldBe 1
    }
  }

  def toDbRecord(transmittedIncomingHistoricalRecordList: List[TransmittedIncomingHistoricalRecord]): List[HistoricalJourneyRecordFromDb] = {
    transmittedIncomingHistoricalRecordList
      .map(x => HistoricalJourneyRecordFromDb(x.journey, x.stopRecords
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
