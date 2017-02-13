package lbt

import akka.actor.ActorSystem
import akka.util.Timeout
import com.rabbitmq.client.ConnectionFactory

import scala.concurrent.duration._

trait RabbitMQConfig  {

  implicit val system = ActorSystem("lbtSystem")
  implicit val timeout: Timeout = 5 seconds
  val connFactory = new ConnectionFactory()
  connFactory.setUri("amqp://guest:guest@localhost/%2F")

}

trait MessageProcessor {
  def apply(message: Array[Byte])
}

class MessageConsumer(processor: MessageProcessor, exchangeName: String, queueName: String, routingKey: String) extends RabbitMQConfig  {

  import akka.actor.{Actor, Props}
  import com.github.sstone.amqp.Amqp._
  import com.github.sstone.amqp.{Amqp, ConnectionOwner, Consumer}
  import scala.concurrent.duration._

  val conn = system.actorOf(ConnectionOwner.props(connFactory, 1 second))
  var messageCounter: Long = 0

  val listener = system.actorOf(Props(new Actor {
    def receive = {
      case Delivery(consumerTag, envelope, properties, body) => {
        processor(body)
        messageCounter += 1
        sender ! Ack(envelope.getDeliveryTag)
      }
    }
  }))

  val consumer = ConnectionOwner.createChildActor(conn, Consumer.props(listener, channelParams = None, autoack = false))
  Amqp.waitForConnection(system, consumer).await()
  consumer ! DeclareExchange(ExchangeParameters(exchangeName, passive = true, "direct"))

  // create a queue
  val queueParams = QueueParameters(queueName, passive = false, durable = true, exclusive = false, autodelete = true)
  consumer ! DeclareQueue(queueParams)
  // bind it
  consumer ! QueueBind(queue = queueName, exchange = exchangeName, routing_key = routingKey)
  // tell our consumer to consume from it
  consumer ! AddQueue(QueueParameters(name = queueName, passive = false, durable = true))

}
