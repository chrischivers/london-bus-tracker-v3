package lbt.datasource

import java.util.concurrent.TimeUnit

import akka.actor.Kill
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import lbt.StandardTestFixture
import lbt.datasource.streaming.DataStreamProcessor
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{Matchers, fixture}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class BusDataSourceTest extends fixture.FunSuite with ScalaFutures with Eventually with Matchers with StrictLogging{

  type FixtureParam = StandardTestFixture

  implicit val executionContext = ExecutionContext.Implicits.global

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(10 seconds),
    interval = scaled(1 second)
  )

  override def withFixture(test: OneArgTest) = {
    val fixture = new StandardTestFixture
    try test(fixture)
    finally {
      fixture.actorSystem.terminate().futureValue
      fixture.testDefinitionsCollection.db.dropDatabase
      fixture.testHistoricalRecordsCollection.db.dropDatabase
      Thread.sleep(1000)
    }
  }

  test("Data stream should be opened and produce a stream of data") { f =>
    val dataStreamProcessingControllerReal = new DataStreamProcessor(f.testDataSourceConfig, f.testMessagingConfig, f.historicalSourceLineProcessor)(f.actorSystem)
    withClue("No data stream returned") {
      dataStreamProcessingControllerReal.start
      Thread.sleep(2000)
      dataStreamProcessingControllerReal.stop

      eventually {
        dataStreamProcessingControllerReal.numberLinesProcessed.futureValue should be > 0L
      }
    }
  }

  test("Data Stream Processing Actor should close http connection and open new one if it restarts") { f =>

    val dataStreamProcessingControllerReal = new DataStreamProcessor(f.testDataSourceConfig, f.testMessagingConfig, f.historicalSourceLineProcessor)(f.actorSystem)
    implicit val timeout = Timeout(FiniteDuration(10, TimeUnit.SECONDS))

    dataStreamProcessingControllerReal.start
    Thread.sleep(2000)
    dataStreamProcessingControllerReal.stop
    Thread.sleep(20000)
    eventually {
    dataStreamProcessingControllerReal.numberLinesProcessed.futureValue should be > 0L
    }
    val numberProcessedBeforeRestart = dataStreamProcessingControllerReal.numberLinesProcessed.futureValue

    val dataStreamProcessingActor = f.actorSystem.actorSelection("user/dataStreamProcessingController/dataStreamProcessingActor").resolveOne().futureValue
    dataStreamProcessingActor ! Kill
    Thread.sleep(1000)
    dataStreamProcessingControllerReal.start
    Thread.sleep(2000)
    dataStreamProcessingControllerReal.stop
    Thread.sleep(20000)
    eventually {
      val numberProcessedAfterRestart = dataStreamProcessingControllerReal.numberLinesProcessedSinceRestart.futureValue
      numberProcessedAfterRestart should be > numberProcessedBeforeRestart
      (numberProcessedAfterRestart + numberProcessedBeforeRestart) shouldEqual dataStreamProcessingControllerReal.numberLinesProcessed.futureValue
    }
  }

  test("Data Stream Processor processes same number of messages as those queued") { f =>

    val dataStreamProcessingControllerReal = new DataStreamProcessor(f.testDataSourceConfig, f.testMessagingConfig, f.historicalSourceLineProcessor)(f.actorSystem)
    dataStreamProcessingControllerReal.start
    Thread.sleep(2000)
    dataStreamProcessingControllerReal.stop
    Thread.sleep(20000)
    eventually {
      val result = dataStreamProcessingControllerReal.numberLinesProcessed.futureValue
      f.historicalSourceLineProcessor.numberSourceLinesProcessed.get() shouldEqual result
    }
  }
}
