import lbt.dataSource.Stream.{DataStreamProcessingController, SourceLine, SourceLinePublisher}
import lbt.{ConfigLoader, MessageConsumer, MessageProcessor, MessagingConfig}
import lbt.historical.HistoricalMessageProcessor
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration._

class DataStreamProcessingTest extends FunSuite {

  val config = ConfigLoader.defaultConfig.messagingConfig

  var lastReceivedMessage: Option[String] = None

  test("Data Stream Processor starts up iterator and starts publishing messages") {

    implicit val patienceConfig =  PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(1, Millis)))

    val messageProcessor = createTestMessageProcessor()
    val consumer = new MessageConsumer(messageProcessor, config.exchangeName, config.historicalRecorderQueueName, config.historicalRecorderRoutingKey)
    DataStreamProcessingController.start()
    eventually {
      lastReceivedMessage.isDefined shouldBe true
      consumer.messageCounter should be > 1.toLong
    }
  }

  test("Data Stream Processor processes same number of messages as those queued") {

    implicit val patienceConfig =  PatienceConfig(timeout = scaled(Span(20, Seconds)), interval = scaled(Span(500, Millis)))

    val messageProcessor = createTestMessageProcessor()
    val consumer = new MessageConsumer(messageProcessor, config.exchangeName, config.historicalRecorderQueueName, config.historicalRecorderRoutingKey)

    DataStreamProcessingController.start()
    Thread.sleep(1000)
    DataStreamProcessingController.stop()
    Thread.sleep(1000)

    eventually {
        DataStreamProcessingController.numberProcessed shouldBe consumer.messageCounter
      lastReceivedMessage.isDefined shouldBe true
    }
    println("Number processed: " + DataStreamProcessingController.numberProcessed + ". Number in queue: " + consumer.messageCounter)
  }


  test("Message should be placed on messaging queue and fetched by consumer") {
    val config = ConfigLoader.defaultConfig.messagingConfig
    val sourceLine = new SourceLine("3", "outbound", "1000", "Brixton", "VHKDJ2D", System.currentTimeMillis())

    val messageProcessor = createTestMessageProcessor()
    val consumer = new MessageConsumer(messageProcessor, config.exchangeName, config.historicalRecorderQueueName, config.historicalRecorderRoutingKey)

    val producer = new SourceLinePublisher()
    producer.publish(sourceLine)
    consumer.messageCounter shouldBe 1
    lastReceivedMessage.get should contain ("VHKDJ2D")
  }


  def createTestMessageProcessor(): MessageProcessor = {
    new MessageProcessor {
      override def apply(message: Array[Byte]): Unit = lastReceivedMessage = Some(message.toString)
    }
  }

}
