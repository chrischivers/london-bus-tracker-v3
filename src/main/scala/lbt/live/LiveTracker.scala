package lbt.live

import lbt.dataSource.Stream.SourceLine
import lbt.{ConfigLoader, MessageConsumer, MessageProcessor}


object LiveTracker {
  val config = ConfigLoader.defaultConfig.messagingConfig
  val liveMessageConsumer = new MessageConsumer(LiveMessageProcessor, config.exchangeName, config.liveTrackerQueueName, config.liveTrackerRoutingKey)
}

object  LiveMessageProcessor extends MessageProcessor {
  override def apply(message: Array[Byte]): Boolean = {
    println("message received")
 //   val jValue = parse(message.toString)
  //  val sourceLine = jValue.extract[SourceLine]
    true

  }
}







