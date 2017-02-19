package lbt

import java.util.concurrent.atomic.AtomicLong

import akka.actor.{Actor, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.github.sstone.amqp.Amqp.{Ack, Delivery, QueueUnbind, _}
import com.github.sstone.amqp.{Amqp, ConnectionOwner, Consumer}
import com.rabbitmq.client.ConnectionFactory
import com.typesafe.scalalogging.StrictLogging
import lbt.dataSource.Stream.SourceLine
import lbt.historical.ValidatedSourceLine

import scala.concurrent.Future
import scala.concurrent.duration._

case class GetNumberMessagesReceived()

trait RabbitMQConfig  {
  implicit val system = ActorSystem("lbtMessagingSystem")
  implicit val timeout: Timeout = 30 seconds
  val connFactory = new ConnectionFactory()
  connFactory.setUri("amqp://guest:guest@localhost/%2F")
}

trait MessageProcessor extends StrictLogging {
  implicit val system = ActorSystem("lbtVehicleSystem")
  def processMessage(message: Array[Byte])
  var lastProcessedMessage: Option[SourceLine] = None
  protected var messagesProcessed: AtomicLong = new AtomicLong(0)
  var lastValidatedMessage: Option[ValidatedSourceLine] = None
  protected var messagesValidated: AtomicLong = new AtomicLong(0)

  def getNumberProcessed = messagesProcessed.get()
  def getNumberValidated = messagesValidated.get()
}

class MessageConsumer(processor: MessageProcessor, val messagingConfig: MessagingConfig) extends RabbitMQConfig  {

  val conn = system.actorOf(ConnectionOwner.props(connFactory, 1 second))

  val listener = system.actorOf(Props(classOf[ListeningActor], processor))

  val consumer = ConnectionOwner.createChildActor(conn, Consumer.props(listener, channelParams = None, autoack = false))
  Amqp.waitForConnection(system, consumer).await()
  consumer ! DeclareExchange(ExchangeParameters(messagingConfig.exchangeName, passive = true, "direct"))

  // create a queue
  val queueParams = QueueParameters(messagingConfig.historicalRecorderQueueName, passive = false, durable = true, exclusive = false, autodelete = true)
  consumer ! DeclareQueue(queueParams)
  // bind it
  consumer ! QueueBind(queue = messagingConfig.historicalRecorderQueueName, messagingConfig.exchangeName, routing_key = messagingConfig.historicalRecorderRoutingKey)
  // tell our consumer to consume from it
  consumer ! AddQueue(QueueParameters(messagingConfig.historicalRecorderQueueName, passive = false, durable = true))

  def getNumberReceived: Future[Long] = (listener ? GetNumberMessagesReceived).mapTo[Long]

  def unbindAndDelete = {
    consumer ! QueueUnbind(messagingConfig.historicalRecorderQueueName, messagingConfig.exchangeName, messagingConfig.historicalRecorderRoutingKey)
    consumer ! DeleteQueue(messagingConfig.historicalRecorderQueueName)
  }

}

class ListeningActor(messageProcessor: MessageProcessor) extends Actor {

    val numberMessagesReceived: AtomicLong = new AtomicLong(0)

    override def receive = {
      case Delivery(consumerTag, envelope, properties, body) => {
        numberMessagesReceived.incrementAndGet()
        messageProcessor.processMessage(body)
        sender ! Ack(envelope.getDeliveryTag)
      }
      case GetNumberMessagesReceived =>
        sender ! numberMessagesReceived.get()
    }
}