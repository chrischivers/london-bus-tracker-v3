import java.io.Serializable

import akka.actor.{Kill, PoisonPill}
import akka.pattern.ask
import lbt.comon._
import lbt.dataSource.Stream._
import lbt.historical.{GetArrivalRecords, ValidatedSourceLine}
import org.scalatest.Matchers._
import org.scalatest._
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration._
import scala.util.Random

class HistoricalRecorderTest extends fixture.FunSuite with ScalaFutures {

  type FixtureParam = TestFixture

  override implicit val patienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(500, Millis)))

  override def withFixture(test: OneArgTest) = {
    val fixture = new TestFixture
    try test(fixture)
    finally {
      fixture.dataStreamProcessingControllerReal ! Stop
      fixture.actorSystem.terminate().futureValue
      fixture.consumer.unbindAndDelete
      fixture.testDefinitionsCollection.db.dropDatabase
    }
  }

  test("Line should be accepted if route is in definitions or disregarded if not in definitions") { f =>

    val routeDefFromDb = f.definitions(f.testBusRoute)
    def randomStop = routeDefFromDb(Random.nextInt(routeDefFromDb.size - 1))
    val arrivalTime = System.currentTimeMillis() + 100000

    val validSourceline = "[1,\"" + randomStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"SampleReg\"," + arrivalTime + "]"
    val invalidSourceLine = "[1,\"" + randomStop.id + "\",\"99XXXX\",2,\"Bromley North\",\"YX62DYN\"," + arrivalTime + "]"
    val testLines = List(validSourceline, invalidSourceLine)

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLines))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually(timeout(30 seconds)) {
      testDataSource.getNumberLinesStreamed shouldBe 2
      f.consumer.getNumberReceived.futureValue shouldBe 2
      f.messageProcessor.getNumberProcessed shouldBe 2
      f.messageProcessor.getNumberValidated shouldBe 1
      testLines should contain(sourceLineBackToLine(f.messageProcessor.lastProcessedMessage.get))
      validSourceline shouldEqual validatedSourceLineBackToLine(f.messageProcessor.lastValidatedMessage.get)
    }
  }

  test("Line should be accepted if bus stop is in definitions and rejected if not") { f =>

    val routeDefFromDb = f.definitions(f.testBusRoute)
    def randomStop = routeDefFromDb(Random.nextInt(routeDefFromDb.size - 1))
    val arrivalTime = System.currentTimeMillis() + 100000

    val validSourceline = "[1,\"" + randomStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"SampleReg\"," + arrivalTime + "]"
    val invalidSourceLine = "[1,\"" + "XXSTOP" + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"SampleReg\"," + arrivalTime + "]"
    val testLines = List(validSourceline, invalidSourceLine)

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLines))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe 2
      f.consumer.getNumberReceived.futureValue shouldBe 2
      f.messageProcessor.getNumberProcessed shouldBe 2
      testLines should contain (sourceLineBackToLine(f.messageProcessor.lastProcessedMessage.get))
      validSourceline shouldEqual validatedSourceLineBackToLine(f.messageProcessor.lastValidatedMessage.get)
      f.messageProcessor.getNumberValidated shouldBe 1
    }
  }

  test("Duplicate lines received within x seconds should not be processed") { f =>

    val routeDefFromDb = f.definitions(f.testBusRoute)
    def randomStop = routeDefFromDb(Random.nextInt(routeDefFromDb.size - 1))
    val arrivalTime = System.currentTimeMillis() + 100000

    val validSourceline = "[1,\"" + randomStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"SampleReg\"," + arrivalTime + "]"
    val testLines = List(validSourceline, validSourceline, validSourceline)

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLines))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe 3
      f.consumer.getNumberReceived.futureValue shouldBe 3
      f.messageProcessor.getNumberProcessed shouldBe 3
      testLines should contain (sourceLineBackToLine(f.messageProcessor.lastProcessedMessage.get))
      validSourceline shouldEqual validatedSourceLineBackToLine(f.messageProcessor.lastValidatedMessage.get)
      f.messageProcessor.getNumberValidated shouldBe 1
    }
  }

  test("Lines received with arrival time in past should be ignored"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute)
    val arrivalTime = System.currentTimeMillis() + Random.nextInt(60000)

    val testLines: List[String] = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"V123456\"," + arrivalTime + "]")

    val splitLines = testLines.splitAt(testLines.size / 2)
    val pastLine = List(splitLines._2.head.replace(arrivalTime.toString, (arrivalTime - 100000).toString))
    val joinedLines = splitLines._1 ++ pastLine ++ splitLines._2.tail

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(joinedLines))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe joinedLines.size
      f.consumer.getNumberReceived.futureValue shouldBe joinedLines.size
      f.messageProcessor.getNumberProcessed shouldBe joinedLines.size
      f.messageProcessor.getNumberValidated shouldBe joinedLines.size - 1
      f.messageProcessor.getCurrentActors.futureValue.size shouldBe 1
    }
  }

  test("An actor should be created for each new vehicle received") { f=>

    val routeDefFromDb = f.definitions(f.testBusRoute)
    def randomStop = routeDefFromDb(Random.nextInt(routeDefFromDb.size - 1))
    val arrivalTime = System.currentTimeMillis() + 100000

    val validSourceline1 = "[1,\"" + randomStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"V123456\"," + arrivalTime + "]"
    val validSourceline2 = "[1,\"" + randomStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"V234567\"," + arrivalTime+ "]"
    val validSourceline3 = "[1,\"" + randomStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"V345678\"," + arrivalTime + "]"
    val validSourceline4 = "[1,\"" + randomStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"V345678\"," + arrivalTime+ "]"

    val testLines = List(validSourceline1, validSourceline2, validSourceline3, validSourceline4)

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLines))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe 4
      f.consumer.getNumberReceived.futureValue shouldBe 4
      f.messageProcessor.getNumberProcessed shouldBe 4
      f.messageProcessor.getNumberValidated shouldBe 4
      f.messageProcessor.getCurrentActors.futureValue.size shouldBe 3
    }
  }

  test("Vehicle actors should add incoming lines relating to arrival times to stop record"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute)
    val arrivalTime = System.currentTimeMillis() + 100000

    val testLines = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"V123456\"," + arrivalTime + "]")

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLines))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe testLines.size
      f.consumer.getNumberReceived.futureValue shouldBe testLines.size
      f.messageProcessor.getNumberProcessed shouldBe testLines.size
      f.messageProcessor.getNumberValidated shouldBe testLines.size
      f.messageProcessor.getCurrentActors.futureValue.size shouldBe 1
      f.messageProcessor.getArrivalRecords("V123456").futureValue.size shouldBe testLines.size
    }
  }

  //TODO test missing records

  test("Vehicle actors should update record when closer arrival time is received"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute)
    val arrivalTime = System.currentTimeMillis() + 100000

    val testLines = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"V123456\"," + arrivalTime + "]")

    val newArrivalTime = arrivalTime - 50000
    val newRecord = testLines.head.replace(arrivalTime.toString, newArrivalTime.toString)
    val testLinesWithUpdated = testLines ++ List(newRecord)

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLinesWithUpdated))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe testLinesWithUpdated.size
      f.consumer.getNumberReceived.futureValue shouldBe testLinesWithUpdated.size
      f.messageProcessor.getNumberProcessed shouldBe testLinesWithUpdated.size
      f.messageProcessor.getNumberValidated shouldBe testLinesWithUpdated.size
      f.messageProcessor.getCurrentActors.futureValue.size shouldBe 1
      f.messageProcessor.getArrivalRecords("V123456").futureValue.apply(routeDefFromDb.head) shouldBe newArrivalTime
    }
  }


  test("Vehicle actors should not persist record if stop arrival time information is missing"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute)
    val arrivalTime = System.currentTimeMillis() + 100000

    val testLines: List[String] = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"V123456\"," + arrivalTime + "]")

    val splitTestLines = testLines.splitAt(testLines.size / 2)
    val testLinesWithMissing = splitTestLines._1 ++ splitTestLines._2.tail

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLinesWithMissing))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe testLinesWithMissing.size
      f.consumer.getNumberReceived.futureValue shouldBe testLinesWithMissing.size
      f.messageProcessor.getNumberProcessed shouldBe testLinesWithMissing.size
      f.messageProcessor.getNumberValidated shouldBe testLinesWithMissing.size
      f.messageProcessor.getCurrentActors.futureValue.size shouldBe 1
      //TODO  check persist

    }
  }

  def sourceLineBackToLine(sourceLine: SourceLine): String = {
    "[1,\"" + sourceLine.stopID + "\",\"" + sourceLine.route + "\"," + sourceLine.direction + ",\"" + sourceLine.destinationText + "\",\"" + sourceLine.vehicleID + "\"," + sourceLine.arrival_TimeStamp + "]"
  }
  def validatedSourceLineBackToLine(sourceLine: ValidatedSourceLine): String = {
    def directionToInt(direction: Direction): Int = direction match {
      case Outbound() => 1
      case Inbound() => 2
    }
    "[1,\"" + sourceLine.busStop.id + "\",\"" + sourceLine.busRoute.id + "\"," + directionToInt(sourceLine.busRoute.direction) + ",\"" + sourceLine.destinationText + "\",\"" + sourceLine.vehicleID + "\"," + sourceLine.arrival_TimeStamp + "]"
  }
}
