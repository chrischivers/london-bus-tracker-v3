import lbt.dataSource.Stream._
import lbt._
import lbt.historical.HistoricalMessageProcessor
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration._

class DataStreamProcessingTest extends FunSuite {

  val testMessagingConfig = ConfigLoader.defaultConfig.messagingConfig.copy(
    exchangeName = "test-lbt-exchange",
    historicalRecorderQueueName = "test-historical-recorder-queue-name",
    historicalRecorderRoutingKey = "test-historical-recorder-routing-key")

  val testDataSourceConfig = ConfigLoader.defaultConfig.dataSourceConfig

  var lastReceivedMessage: Option[String] = None

  test("Data Stream Processor starts up iterator and starts publishing messages") {

    implicit val patienceConfig =  PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(1, Millis)))

    val messageProcessor = createTestMessageProcessor()
    val consumer = new MessageConsumer(messageProcessor, testMessagingConfig.exchangeName, testMessagingConfig.historicalRecorderQueueName, testMessagingConfig.historicalRecorderRoutingKey)
    DataStreamProcessingController(testMessagingConfig) ! Start
    eventually {
      lastReceivedMessage.isDefined shouldBe true
      consumer.messageCounter should be > 1.toLong
    }
  }

  test("Data Stream Processor processes same number of messages as those queued") {

    implicit val patienceConfig =  PatienceConfig(timeout = scaled(Span(20, Seconds)), interval = scaled(Span(500, Millis)))

    val messageProcessor = createTestMessageProcessor()
    val consumer = new MessageConsumer(messageProcessor, testMessagingConfig.exchangeName, testMessagingConfig.historicalRecorderQueueName, testMessagingConfig.historicalRecorderRoutingKey)

    val dataStreamProcessingController = DataStreamProcessingController(testMessagingConfig)
    dataStreamProcessingController ! Start
    Thread.sleep(1000)
    dataStreamProcessingController ! Stop
    Thread.sleep(1000)

    eventually {
      dataStreamProcessingController ! GetNumberProcessed shouldBe consumer.messageCounter
      lastReceivedMessage.isDefined shouldBe true
    }
  }


  test("Message should be placed on messaging queue and fetched by consumer") {

    val messageProcessor = createTestMessageProcessor()
    val consumer = new MessageConsumer(messageProcessor, testMessagingConfig.exchangeName, testMessagingConfig.historicalRecorderQueueName, testMessagingConfig.historicalRecorderRoutingKey)

    val testDataSource = new TestDataSource()
    val dataStreamProcessingController = DataStreamProcessingController(testDataSource, testMessagingConfig)

    dataStreamProcessingController ! Start
    Thread.sleep(1000)
    dataStreamProcessingController ! Stop
    Thread.sleep(1000)

    consumer.messageCounter shouldBe testDataSource.numberLinesProcessed
    testDataSource.lineList should contain (lastReceivedMessage.get)
  }

  test("Message should be received and stored in database") {
    val sourceLine = SourceLine("3", "outbound", "1000", "Brixton", "VHKDJ2D", System.currentTimeMillis())

    val messageProcessor = createTestMessageProcessor()
    val consumer = new MessageConsumer(messageProcessor, testMessagingConfig.exchangeName, testMessagingConfig.historicalRecorderQueueName, testMessagingConfig.historicalRecorderRoutingKey)
    //TODO HERE
  }


  def createTestMessageProcessor(): MessageProcessor = {
    new MessageProcessor {
      override def apply(message: Array[Byte]): Unit = lastReceivedMessage = Some(new String(message, "UTF-8"))
    }
  }

    class TestDataSource(config: DataSourceConfig = testDataSourceConfig) extends BusDataSource(config) {

      var numberLinesProcessed: Int = 0

      val lineList = List[String](
        "[1,\"490009774E\",\"352\",2,\"Bromley North\",\"YX62DYN\",1487026559000]",
        "[1,\"490010622BA\",\"74\",2,\"Putney\",\"LX05EYM\",1487026515000]",
        "[1,\"490012612C\",\"329\",2,\"Enfield\",\"LJ08CTZ\",1487027221000]",
        "[1,\"490000043CB\",\"31\",1,\"White City\",\"LK04HXU\",1487026106000]",
        "[1,\"490007220Z\",\"55\",2,\"Bakers Arms\",\"LTZ1463\",1487027598000]"
      )
      //TODO update current time

      val streamingLineList = Stream.continually(lineList.toStream).flatten

      override def hasNext: Boolean = true

      override def next() = {
        numberLinesProcessed += 1
        SourceLineValidator(streamingLineList.take(1).head)
      }

      override def reloadDataSource() = Unit //Do Nothing
    }

}
