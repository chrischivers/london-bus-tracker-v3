import akka.actor.PoisonPill
import lbt.dataSource.Stream._
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
      fixture.dataStreamProcessingControllerReal ! PoisonPill
      fixture.consumer.unbindAndDelete
      fixture.testDefinitionsCollection.db.dropDatabase
    }
  }

  test("Line should be accepted if route is in definitions or disregarded if not in definitions") { f =>

    val routeDefFromDb = f.definitions(f.testBusRoute)
    val randomStop = routeDefFromDb(Random.nextInt(routeDefFromDb.size - 1))

    val validSourceline = "[1,\"" + randomStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"SampleReg\"," + System.currentTimeMillis() + "]"
    val invalidSourceLine = "[1,\"" + randomStop.id + "\",\"99XXXX\",2,\"Bromley North\",\"YX62DYN\"," + System.currentTimeMillis() + "]"
    val testLines = List(validSourceline, invalidSourceLine)

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLines))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually(timeout(30 seconds)) {
      testDataSource.getNumberLinesStreamed shouldBe 2
      f.consumer.getNumberReceived.futureValue shouldBe 2
      f.messageProcessor.getNumberProcessed shouldBe 2
      f.messageProcessor.getNumberValidated shouldBe 1
      testLines should contain(sourceLineBackToLine(f.messageProcessor.lastProcessedMessage.get))
      validSourceline shouldEqual sourceLineBackToLine(f.messageProcessor.lastValidatedMessage.get)
    }
  }

  test("Line should be accepted if bus stop is in definitions and rejected if not") { f =>

    val routeDefFromDb = f.definitions(f.testBusRoute)
    val randomStop = routeDefFromDb(Random.nextInt(routeDefFromDb.size - 1))


    val validSourceline = "[1,\"" + randomStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"SampleReg\"," + System.currentTimeMillis() + "]"
    val invalidSourceLine = "[1,\"" + "XXSTOP" + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"SampleReg\"," + System.currentTimeMillis() + "]"
    val testLines = List(validSourceline, invalidSourceLine)

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLines))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe 2
      f.consumer.getNumberReceived.futureValue shouldBe 2
      f.messageProcessor.getNumberProcessed shouldBe 2
      testLines should contain (sourceLineBackToLine(f.messageProcessor.lastProcessedMessage.get))
      validSourceline shouldEqual sourceLineBackToLine(f.messageProcessor.lastValidatedMessage.get)
      f.messageProcessor.getNumberValidated shouldBe 1
    }
  }

  test("Duplicate lines received within 30 seconds should not be processed") { f =>

    val routeDefFromDb = f.definitions(f.testBusRoute)
    val randomStop = routeDefFromDb(Random.nextInt(routeDefFromDb.size - 1))

    val validSourceline = "[1,\"" + randomStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"SampleReg\"," + System.currentTimeMillis() + "]"
    val testLines = List(validSourceline, validSourceline, validSourceline)

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLines))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe 3
      f.consumer.getNumberReceived.futureValue shouldBe 3
      f.messageProcessor.getNumberProcessed shouldBe 3
      testLines should contain (sourceLineBackToLine(f.messageProcessor.lastProcessedMessage.get))
      validSourceline shouldEqual sourceLineBackToLine(f.messageProcessor.lastValidatedMessage.get)
      f.messageProcessor.getNumberValidated shouldBe 1
    }
  }

  def sourceLineBackToLine(sourceLine: SourceLine): String = {
    "[1,\"" + sourceLine.stopID + "\",\"" + sourceLine.route + "\"," + sourceLine.direction + ",\"" + sourceLine.destinationText + "\",\"" + sourceLine.vehicleID + "\"," + sourceLine.arrival_TimeStamp + "]"
  }
}
