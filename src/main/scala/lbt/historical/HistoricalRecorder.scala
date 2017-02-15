package lbt.historical

import lbt.dataSource.Stream.SourceLine
import lbt.{ConfigLoader, MessageConsumer, MessageProcessor, MessagingConfig}
import net.liftweb.json.{DefaultFormats, parse}


object HistoricalRecorder {
  val defaultMessagingConfig = ConfigLoader.defaultConfig.messagingConfig
  val defaultHistoricalMessageProcessor = new HistoricalMessageProcessor

  def apply(): MessageConsumer = apply(defaultHistoricalMessageProcessor, defaultMessagingConfig)
  def apply(historicalMessageProcessor: HistoricalMessageProcessor, messagingConfig: MessagingConfig) =
    new MessageConsumer(historicalMessageProcessor, messagingConfig.exchangeName, messagingConfig.historicalRecorderQueueName, messagingConfig.historicalRecorderRoutingKey)
}

class HistoricalMessageProcessor extends MessageProcessor {

  var lastProcessedMessage: Option[SourceLine] = None
  var messagesProcessed: Long = 0
  var lastValidatedMessage: Option[SourceLine] = None
  var messagesValidated: Long = 0

  implicit val formats = DefaultFormats
  override def apply(message: Array[Byte]): Boolean = {

    val jValue = parse(new String(message, "UTF-8"))
    val sourceLine = jValue.extract[SourceLine]
    lastProcessedMessage = Some(sourceLine)
    messagesProcessed += 1
    println(s"message received: $sourceLine")
    true
  }

  def existsInRouteDefinition: Boolean = true

  def notOnIgnoreList: Boolean = true
}







