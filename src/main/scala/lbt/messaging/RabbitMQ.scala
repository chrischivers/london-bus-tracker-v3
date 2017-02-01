package lbt.messaging

import akka.actor.{Props, ActorSystem, ActorRef}
import com.rabbitmq.client.AMQP.Exchange
import com.thenewmotion.akka.rabbitmq._
import lbt.dataSource.SourceLine
import lbt.{MessagingConfig, ConfigLoader}
import concurrent.duration._
import scala.concurrent.{Future, ExecutionContext}

object RabbitMQ  {

  val config = ConfigLoader.defaultConfig.messagingConfig
  //private var rabbitMq:RabbitMQ = new RabbitMQ(config)

  implicit val system = ActorSystem("LBTSystem")

  val connFactory = new ConnectionFactory


  val connection = system.actorOf(ConnectionActor.props(connFactory), "rabbitMQ")
  val exchange = config.exchangeName
  val assocQueue = config.dataStreamQueueName

  // Defining and creating the Channel and actor publisher
  def setupPublisher(channel: Channel, self: ActorRef) {
    val queue = channel.queueDeclare(assocQueue, true, false, false, null).getQueue
    channel.queueBind(queue, exchange, "")
  }
  connection ! CreateChannel(ChannelActor.props(setupPublisher), Some("publisher"))

  // Defining and creating the Channel and actor subscriber
  def setupSubscriber(channel: Channel, self: ActorRef) {
    val queue = channel.queueDeclare(assocQueue, true, false, false, null).getQueue
    channel.queueBind(queue, exchange, "")
    val consumer = new DefaultConsumer(channel)
    // {
     // override def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]) {
    //    Json.parse(body).validate[SourceLine].asOpt.foreach( RabbitMQ.publish )
    //  }
    //}
    channel.basicConsume(queue, true, consumer)
  }
  connection ! CreateChannel(ChannelActor.props(setupSubscriber), Some("subscriber"))

  val publisher = system.actorSelection("/user/rabbitMQ/publisher")

  // Publishing a few messages to the RabbitMQ
  def publish(sourceLine: SourceLine)(channel: Channel) {
    channel.basicPublish(
      exchange, "", null,
      "Test".getBytes("UTF-8"))
  }

  def publishMessage(sourceLine:SourceLine) = {
    publisher ! ChannelMessage(publish(sourceLine), dropIfNoChannel = false)
  }




  def fromBytes(x: Array[Byte]) = new String(x, "UTF-8")

  def sourceLineToString(sourceLine: SourceLine): String = {
    //route: String, direction: String, stopID: String, destinationText: String, vehicleID: String, arrival_TimeStamp: Long)
    sourceLine.route + "," +
      sourceLine.direction + "," +
      sourceLine.stopID + "," +
      sourceLine.destinationText + "," +
      sourceLine.vehicleID + "," +
      sourceLine.arrival_TimeStamp
  }
}



/*

  implicit val system = ActorSystem()
  val factory = new ConnectionFactory()
  val connection = system.actorOf(ConnectionActor.props(factory), "rabbitmq")
  val exchange = config.exchangeName
  val routingKey = "routingKey"


  def setupPublisher(channel: Channel, self: ActorRef) {
    val queue = channel.queueDeclare(config.dataStreamQueueName, true, false, false, new java.util.HashMap[String, Object]()).getQueue
    channel.queueBind(queue, exchange, routingKey)
  }
  connection ! CreateChannel(ChannelActor.props(setupPublisher), Some("publisher"))

  val publisher = system.actorSelection("/user/rabbitmq/publisher")

  def publishDataSourceLine(sourceLine: SourceLine) = {
    def publish(channel: Channel) = {
      channel.basicPublish(exchange, routingKey, null, sourceLineToString(sourceLine).getBytes("UTF-8"))
    }
    publisher ! ChannelMessage(publish, dropIfNoChannel = false)
  }

}*/