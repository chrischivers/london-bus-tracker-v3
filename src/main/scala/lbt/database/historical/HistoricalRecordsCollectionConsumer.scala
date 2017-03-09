package lbt.database.historical

import java.util.concurrent.atomic.AtomicLong

import akka.actor.{Actor, ActorSystem, Props}
import com.github.sstone.amqp.Amqp.{Ack, Delivery, QueueUnbind, _}
import com.github.sstone.amqp.{Amqp, ConnectionOwner, Consumer}
import com.rabbitmq.client.ConnectionFactory
import lbt.historical.RecordedVehicleDataToPersist
import net.liftweb.json.{DefaultFormats, _}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import lbt.MessagingConfig

import scala.concurrent.Future
import scala.concurrent.duration._

case class GetNumberMessagesConsumed()

class HistoricalRecordsCollectionConsumer(messagingConfig: MessagingConfig, historicalRecordsCollection: HistoricalRecordsCollection)(implicit actorSystem: ActorSystem) extends StrictLogging {

  val formats = DefaultFormats
  implicit val timeout:Timeout = 10 seconds

  val connFactory = new ConnectionFactory()
  connFactory.setUri(messagingConfig.rabbitUrl)
  val conn = actorSystem.actorOf(ConnectionOwner.props(connFactory, 1 second))


  val listener = actorSystem.actorOf(Props(classOf[ListeningActor], historicalRecordsCollection))
  val consumer = ConnectionOwner.createChildActor(conn, Consumer.props(listener, channelParams = None, autoack = false))
  Amqp.waitForConnection(actorSystem, consumer).await()
  consumer ! DeclareExchange(ExchangeParameters(messagingConfig.exchangeName, passive = true, "direct"))

  // create a queue
  val queueParams = QueueParameters(messagingConfig.historicalDBInsertQueueName, passive = false, durable = true, exclusive = false, autodelete = true)
  consumer ! DeclareQueue(queueParams)
  // bind it
  consumer ! QueueBind(queue = messagingConfig.historicalDBInsertQueueName, messagingConfig.exchangeName, routing_key = messagingConfig.historicalDbRoutingKey)
  // tell our consumer to consume from it
  consumer ! AddQueue(QueueParameters(messagingConfig.historicalDBInsertQueueName, passive = false, durable = true))

  //  def getNumberReceived: Future[Long] = (listener ? GetNumberMessagesReceived).mapTo[Long]

  def unbindAndDelete = {
    consumer ! QueueUnbind(messagingConfig.historicalDBInsertQueueName, messagingConfig.exchangeName, messagingConfig.historicalDBInsertQueueName)
    consumer ! DeleteQueue(messagingConfig.historicalDBInsertQueueName)
  }

  def getNumberMessagesConsumed: Future[Long] = (listener ? GetNumberMessagesConsumed).mapTo[Long]
}

class ListeningActor(historicalRecordsCollection: HistoricalRecordsCollection) extends Actor with StrictLogging {

  implicit val formats = DefaultFormats
  val numberMessagesReceived: AtomicLong = new AtomicLong(0)

  override def receive = {
    case Delivery(consumerTag, envelope, properties, body) => {
      numberMessagesReceived.incrementAndGet()
      historicalRecordsCollection.insertHistoricalRecordIntoDB(parse(new String(body, "UTF-8")).extract[RecordedVehicleDataToPersist])
      logger.info("Record to persist: " + parse(new String(body, "UTF-8")).extract[RecordedVehicleDataToPersist])
      sender ! Ack(envelope.getDeliveryTag)
    }
    case GetNumberMessagesConsumed => sender ! numberMessagesReceived.get()
  }
}