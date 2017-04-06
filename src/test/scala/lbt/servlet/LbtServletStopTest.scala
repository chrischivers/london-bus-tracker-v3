package lbt.servlet

import akka.actor.Kill
import lbt.{TransmittedIncomingHistoricalRecord, TransmittedIncomingHistoricalStopRecord}
import lbt.database.historical.{ArrivalRecord, HistoricalJourneyRecordFromDb, HistoricalStopRecordFromDb}
import net.liftweb.json._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, Matchers}
import org.scalatra.test.scalatest.ScalatraFunSuite

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._


class LbtServletStopTest extends ScalatraFunSuite with ScalaFutures with Matchers with BeforeAndAfterAll with Eventually with LbtServletTestFixture {

  implicit val formats = DefaultFormats
  implicit val ec = ExecutionContext.Implicits.global
  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(30 seconds),
    interval = scaled(1 second))

  addServlet(new LbtServlet(testDefinitionsTable, testHistoricalTable, dataStreamProcessor, historicalSourceLineProcessor, vehicleActorSupervisor, historicalRecordsFetcher), "/*")

  Thread.sleep(10000)

  test("should produce a list of arrival history for a given stop") {
    testBusRoutes.foreach(route => {
      val routeDef = definitions(route)
      routeDef.foreach(stop => {
        get("/stop/" + stop.stopID) {
          status should equal(200)
          println("BODY: " + body)
          toDbRecord(parse(body).extract[List[TransmittedIncomingHistoricalStopRecord]]) should equal(testHistoricalTable.getHistoricalRecordFromDbByStop(stop.stopID).futureValue)
          parse(body).extract[List[TransmittedIncomingHistoricalStopRecord]].foreach(record => {
            record.stopID shouldEqual stop.stopID
            record.journey.busRoute shouldEqual route
          })
          parse(body).extract[List[TransmittedIncomingHistoricalStopRecord]].size shouldBe testBusRoutes.flatMap(route => definitions(route)).count(route => route.stopID == stop.stopID)
        }
      })
    })
  }

  test("should produce an empty list for an unknown stop ID") {
    get("/stop/" + "UNKNOWN") {
      status should equal(400)
    }
  }

  //TODO more tests here checking variables

  def toDbRecord(transmittedIncomingHistoricalStopRecordList: List[TransmittedIncomingHistoricalStopRecord]): List[HistoricalStopRecordFromDb] = {
    transmittedIncomingHistoricalStopRecordList
      .map(x => HistoricalStopRecordFromDb(x.stopID, x.arrivalTime, x.journey))
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
