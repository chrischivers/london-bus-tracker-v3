import akka.util.Timeout
import lbt.dataSource.Stream._
import lbt._
import lbt.database.DbCollections
import lbt.historical.{HistoricalMessageProcessor, HistoricalRecorder}
import net.liftweb.json.{DefaultFormats, parse}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.Matchers.{message, _}
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Millis, Seconds, Span}

import scala.language.postfixOps
import akka.pattern.ask

import scala.concurrent.Await
import scala.concurrent.duration._

class DataStreamProcessingTest extends FunSuite with BeforeAndAfter {

  before {
    BusDataSource.reloadDataSource()
  }

  val testMessagingConfig = ConfigLoader.defaultConfig.messagingConfig.copy(
    exchangeName = "test-lbt-exchange",
    historicalRecorderQueueName = "test-historical-recorder-queue-name",
    historicalRecorderRoutingKey = "test-historical-recorder-routing-key")

  val testDataSourceConfig = ConfigLoader.defaultConfig.dataSourceConfig

  test("Data Stream Processor starts up iterator and starts publishing messages") {

    implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(500, Millis)))

    val messageProcessor = new HistoricalMessageProcessor()
    val consumer = HistoricalRecorder(messageProcessor, testMessagingConfig)

    DataStreamProcessingController(testMessagingConfig) ! Start
    eventually {
      messageProcessor.lastProcessedMessage.isDefined shouldBe true
      consumer.messagesReceived should be > 1.toLong
    }
  }

  test("Data Stream Processor processes same number of messages as those queued") {

    implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(60, Seconds)), interval = scaled(Span(500, Millis)))
    implicit val timeout = Timeout(5 seconds)

    val messageProcessor = new HistoricalMessageProcessor()
    val consumer = HistoricalRecorder(messageProcessor, testMessagingConfig)

    val dataStreamProcessingController = DataStreamProcessingController(testMessagingConfig)
    dataStreamProcessingController ! Start
    Thread.sleep(500)
    dataStreamProcessingController ! Stop

    eventually {
      val result = Await.result(dataStreamProcessingController ? GetNumberProcessed, timeout.duration).asInstanceOf[Long]
       result shouldBe consumer.messagesReceived
      messageProcessor.lastProcessedMessage.isDefined shouldBe true
    }
  }

  test("Messages should be placed on messaging queue and fetched by consumer") {

    implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(30, Seconds)), interval = scaled(Span(500, Millis)))

    val messageProcessor = new HistoricalMessageProcessor()
    val consumer = HistoricalRecorder(messageProcessor, testMessagingConfig)

    val testDataSource = new TestDataSource(testDataSourceConfig)
    val dataStreamProcessingController = DataStreamProcessingController(testDataSource, testMessagingConfig)

    dataStreamProcessingController ! Start
    Thread.sleep(500)
    dataStreamProcessingController ! Stop

    eventually {
      consumer.messagesReceived shouldBe testDataSource.numberLinesStreamed
      messageProcessor.messagesProcessed shouldBe testDataSource.numberLinesStreamed
      testDataSource.testLines should contain(soureLineBackToLine(messageProcessor.lastProcessedMessage.get))
    }

    def soureLineBackToLine(sourceLine: SourceLine): String = {
      "[1,\"" + sourceLine.stopID + "\",\"" + sourceLine.route + "\"," + sourceLine.direction + ",\"" +  sourceLine.destinationText + "\",\"" +  sourceLine.vehicleID + "\"," +  sourceLine.arrival_TimeStamp + "]"
    }
  }

  test("Message should be received and stored in database") {
    //TODO
  }



}
