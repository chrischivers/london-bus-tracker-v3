package lbt.live

import lbt.dataSource.SourceLine
import lbt.{ConfigLoader, MessageConsumer, MessageProcessor}
import net.liftweb.json._


object LiveTracker {
  val config = ConfigLoader.defaultConfig.messagingConfig
  val liveMessageConsumer = new MessageConsumer(LiveMessageProcessor, config.exchangeName, config.liveTrackerQueueName, config.liveTrackerRoutingKey)
}

object  LiveMessageProcessor extends MessageProcessor {
  implicit val formats = DefaultFormats
  override def apply(message: Array[Byte]): Unit = {
    println("message received")
    val jValue = parse(message.toString)
    val sourceLine = jValue.extract[SourceLine]

  }
}







