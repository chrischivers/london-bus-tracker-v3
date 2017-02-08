package lbt.historical

import lbt.dataSource.SourceLine
import lbt.{ConfigLoader, MessageConsumer, MessageProcessor}
import net.liftweb.json._


object HistoricalRecorder {
  val config = ConfigLoader.defaultConfig.messagingConfig
  val historicalMessageConsumer = new MessageConsumer(HistoricalMessageProcessor, config.exchangeName, config.historicalRecorderQueueName, config.historicalRecorderRoutingKey)
}

object  HistoricalMessageProcessor extends MessageProcessor {
  implicit val formats = DefaultFormats
  override def apply(message: Array[Byte]): Unit = {
    println("message received")
    val jValue = parse(message.toString)
    val sourceLine = jValue.extract[SourceLine]

  }
}







