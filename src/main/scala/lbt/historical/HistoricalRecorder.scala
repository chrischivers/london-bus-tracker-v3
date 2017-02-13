package lbt.historical

import lbt.dataSource.Stream.SourceLine
import lbt.{ConfigLoader, MessageConsumer, MessageProcessor}
import play.api.libs.json.Json


object HistoricalRecorder {
  val config = ConfigLoader.defaultConfig.messagingConfig
  val historicalMessageConsumer = new MessageConsumer(HistoricalMessageProcessor, config.exchangeName, config.historicalRecorderQueueName, config.historicalRecorderRoutingKey)
}

object  HistoricalMessageProcessor extends MessageProcessor {
  override def apply(message: Array[Byte]): Unit = {
    println("message received")
   // val jValue = Json.parse(message.toString)
    // val sourceLine = jValue.extract[SourceLine]

  }

  def existsInRouteDefinition: Boolean = true

  def notOnIgnoreList: Boolean = true
}







