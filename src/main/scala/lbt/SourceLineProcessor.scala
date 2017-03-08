package lbt

import java.util.concurrent.atomic.AtomicLong

import com.typesafe.scalalogging.StrictLogging
import lbt.datasource.SourceLine
import lbt.historical.ValidatedSourceLine

//case class GetNumberMessagesReceived()
//
//trait RabbitMQConfig  {
//  implicit val timeout: Timeout = 30 seconds
//  val connFactory = new ConnectionFactory()
//  connFactory.setUri("amqp://guest:guest@localhost/%2F")
//  //TODO put in config
//}

trait SourceLineProcessor extends StrictLogging {
  def processSourceLine(sourceLine: SourceLine)
  var lastProcessedSourceLine: Option[SourceLine] = None
  val numberSourceLinesProcessed: AtomicLong = new AtomicLong(0)
  var lastValidatedSourceLine: Option[ValidatedSourceLine] = None
  val numberSourceLinesValidated: AtomicLong = new AtomicLong(0)
}

//class MessageConsumer(processor: SourceLineProcessor, val messagingConfig: MessagingConfig)(implicit actorSystem: ActorSystem) extends RabbitMQConfig  {
//
//  val conn = actorSystem.actorOf(ConnectionOwner.props(connFactory, 1 second))
//
//  val listener = actorSystem.actorOf(Props(classOf[ListeningActor], processor))
//
//  val consumer = ConnectionOwner.createChildActor(conn, Consumer.props(listener, channelParams = None, autoack = false))
//  Amqp.waitForConnection(actorSystem, consumer).await()
//  consumer ! DeclareExchange(ExchangeParameters(messagingConfig.exchangeName, passive = true, "direct"))
//
//  // create a queue
//  val queueParams = QueueParameters(messagingConfig.historicalRecorderQueueName, passive = false, durable = true, exclusive = false, autodelete = true)
//  consumer ! DeclareQueue(queueParams)
//  // bind it
//  consumer ! QueueBind(queue = messagingConfig.historicalRecorderQueueName, messagingConfig.exchangeName, routing_key = messagingConfig.historicalRecorderRoutingKey)
//  // tell our consumer to consume from it
//  consumer ! AddQueue(QueueParameters(messagingConfig.historicalRecorderQueueName, passive = false, durable = true))
//
//  def getNumberReceived: Future[Long] = (listener ? GetNumberMessagesReceived).mapTo[Long]
//
//  def unbindAndDelete = {
//    consumer ! QueueUnbind(messagingConfig.historicalRecorderQueueName, messagingConfig.exchangeName, messagingConfig.historicalRecorderRoutingKey)
//    consumer ! DeleteQueue(messagingConfig.historicalRecorderQueueName)
//  }
//
//}
//
//class ListeningActor(messageProcessor: SourceLineProcessor) extends Actor {
//
//    val numberMessagesReceived: AtomicLong = new AtomicLong(0)
//
//    override def receive = {
//      case Delivery(consumerTag, envelope, properties, body) => {
//        numberMessagesReceived.incrementAndGet()
//        messageProcessor.processSourceLine(body)
//        sender ! Ack(envelope.getDeliveryTag)
//      }
//      case GetNumberMessagesReceived =>
//        sender ! numberMessagesReceived.get()
//    }
//}