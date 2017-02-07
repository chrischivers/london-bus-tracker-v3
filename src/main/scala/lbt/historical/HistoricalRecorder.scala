package lbt.historical

import akka.actor.{Actor, Props}
import com.github.sstone.amqp.Amqp._
import com.github.sstone.amqp.{Amqp, Consumer, ConnectionOwner}
import lbt.{ConfigLoader, MessageConsumer, MessageProcessor, RabbitMQ}


object HistoricalRecorder {
  val config = ConfigLoader.defaultConfig.messagingConfig
  val messageConsumer = new MessageConsumer(HistoricalMessageProcessor, config.exchangeName, config.historicalRecorderQueueName, config.historicalRecorderRoutingKey)
}

object  HistoricalMessageProcessor extends MessageProcessor {
  override def apply(message: Array[Byte]): Unit = {
      println("message received")
  }
}







