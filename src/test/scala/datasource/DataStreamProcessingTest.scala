package datasource

import historical.HistoricalTestFixture
import lbt.comon.{Start, Stop}
import lbt.datasource.SourceLine
import lbt.datasource.streaming.{DataStreamProcessingController, DataStreamProcessor, GetNumberLinesProcessed}
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.fixture
import akka.pattern.ask
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration._
import scala.language.postfixOps

class DataStreamProcessingTest extends fixture.FunSuite with ScalaFutures{

  type FixtureParam = HistoricalTestFixture

  override implicit val patienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(500, Millis)))

  override def withFixture(test: OneArgTest) = {
    val fixture = new HistoricalTestFixture
    try test(fixture)
    finally {
      fixture.dataStreamProcessingControllerReal.stop
      fixture.actorSystem.terminate().futureValue
      fixture.testDefinitionsCollection.db.dropDatabase
      Thread.sleep(5000)
    }
  }

  test("Data Stream Processor processes same number of messages as those queued") { f =>

    f.dataStreamProcessingControllerReal.start
    Thread.sleep(60000)
    f.dataStreamProcessingControllerReal.stop

    eventually(timeout(40 seconds)) {
    val result = f.dataStreamProcessingControllerReal.numberLinesProcessed.futureValue
      result shouldBe f.historicalSourceLineProcessor.getNumberProcessed
      f.historicalSourceLineProcessor.lastProcessedSourceLine.isDefined shouldBe true
    }
  }
}
