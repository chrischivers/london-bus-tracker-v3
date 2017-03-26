package lbt.servlet

import akka.actor.Kill
import lbt.TransmittedIncomingHistoricalRecord
import lbt.comon.{BusRoute, BusStop}
import lbt.database.historical.{ArrivalRecord, HistoricalJourneyRecordFromDb, Journey}
import net.liftweb.json._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FunSuite, FunSuiteLike, Matchers}
import org.scalatra.test.scalatest.{ScalatraFunSuite, ScalatraSuite}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext


class LbtServletVehicleTest extends ScalatraFunSuite with ScalaFutures with Matchers with BeforeAndAfterAll with Eventually with LbtServletTestFixture {

  implicit val formats = DefaultFormats
  implicit val ec = ExecutionContext.Implicits.global
  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(30 seconds),
    interval = scaled(1 second))

  addServlet(new LbtServlet(testDefinitionsTable, testHistoricalTable, dataStreamProcessor, historicalSourceLineProcessor), "/*")

  Thread.sleep(10000)

  test("should produce a list of vehicles and their arrival times for a given vehicle ID") {
      get("/vehicle/" + vehicleReg) {
        status should equal(200)
        println("BODY: " + body)
        toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalRecord]]) should equal(testHistoricalTable.getHistoricalRecordFromDbByVehicle(vehicleReg))
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].foreach(record => {
          record.journey.vehicleReg shouldEqual vehicleReg
          record.stopRecords.map(stopRecs => stopRecs.busStop) shouldEqual definitions(record.journey.busRoute)
        })
        parse(body).extract[List[TransmittedIncomingHistoricalRecord]].size shouldBe testBusRoutes.size
      }
  }

  test("should produce an empty list for an unknown vehicle ID") {
    get("/vehicle/" + "UNKNOWN") {
      status should equal(200)
      parse(body).extract[List[TransmittedIncomingHistoricalRecord]].isEmpty shouldBe true
    }
  }

  //TODO more tests here checking variables

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
