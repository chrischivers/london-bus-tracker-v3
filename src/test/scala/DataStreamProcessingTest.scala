import akka.actor.PoisonPill
import akka.pattern.ask
import lbt.comon.{Start, Stop}
import lbt.dataSource.Stream._
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.fixture
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration._
import scala.language.postfixOps

class DataStreamProcessingTest extends fixture.FunSuite with ScalaFutures{

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
      Thread.sleep(5000)
    }
  }

  test("Data Stream Processor processes same number of messages as those queued") { f =>

    f.dataStreamProcessingControllerReal ! Start
    Thread.sleep(60000)
    f.dataStreamProcessingControllerReal ! Stop

    eventually(timeout(40 seconds)) {
    val result = (f.dataStreamProcessingControllerReal ? GetNumberLinesProcessed)(20 seconds).futureValue.asInstanceOf[Long]
       result shouldBe f.consumer.getNumberReceived.futureValue
      result shouldBe f.messageProcessor.getNumberProcessed
      f.messageProcessor.lastProcessedMessage.isDefined shouldBe true
    }
  }

  test("Messages should be placed on messaging queue and fetched by consumer") { f =>

    val testDataSource = new TestDataSource(f.testDataSourceConfig)
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      f.consumer.getNumberReceived.futureValue shouldBe testDataSource.getNumberLinesStreamed
      f.messageProcessor.getNumberProcessed shouldBe testDataSource.getNumberLinesStreamed
      testDataSource.testLines should contain(sourceLineBackToLine(f.messageProcessor.lastProcessedMessage.get))
    }

    def sourceLineBackToLine(sourceLine: SourceLine): String = {
      "[1,\"" + sourceLine.stopID + "\",\"" + sourceLine.route + "\"," + sourceLine.direction + ",\"" +  sourceLine.destinationText + "\",\"" +  sourceLine.vehicleID + "\"," +  sourceLine.arrival_TimeStamp + "]"
    }
  }




}
