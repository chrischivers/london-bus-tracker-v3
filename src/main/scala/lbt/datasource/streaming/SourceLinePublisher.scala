package lbt.datasource.streaming

//import java.util.concurrent.TimeUnit
//import java.util.concurrent.atomic.AtomicLong
//
//import akka.actor.ActorSystem
//import com.github.sstone.amqp.Amqp._
//import com.github.sstone.amqp.{ChannelOwner, ConnectionOwner}
//import lbt.datasource.SourceLine
//import lbt.{MessagingConfig, RabbitMQConfig}
//import net.liftweb.json.Serialization.write
//import net.liftweb.json._
//
//import scala.concurrent.duration._

//class SourceLinePublisher(config: MessagingConfig)(implicit actorSystem: ActorSystem) extends RabbitMQConfig  {
//
//  val conn = actorSystem.actorOf(ConnectionOwner.props(connFactory, 1 second))
//  val producer = ConnectionOwner.createChildActor(conn, ChannelOwner.props())
//
//  val messagesPublished = new AtomicLong(0)
//
//  waitForConnection(actorSystem, conn, producer).await(10, TimeUnit.SECONDS)
//
//  producer ! DeclareExchange(ExchangeParameters(config.exchangeName, passive = true, "direct"))
//
//  def publish (sourceLine: SourceLine) = {
//    val sourceLineBytes = sourceLineToJson(sourceLine).getBytes
//    producer ! Publish(config.exchangeName, config.historicalRecorderRoutingKey, sourceLineBytes, properties = None, mandatory = true, immediate = false)
//    messagesPublished.incrementAndGet()
//    //producer ! Publish(config.exchangeName, config.liveTrackerRoutingKey, sourceLineBytes, properties = None, mandatory = true, immediate = false)
//  }
//
//  def sourceLineToJson(sourceLine: SourceLine): String = {
//    implicit val formats = DefaultFormats
//    write(sourceLine)
//    //sourceLine.toString
//    //route: String, direction: String, stopID: String, destinationText: String, vehicleID: String, arrival_TimeStamp: Long)
//  }
//
//  def getNumberMessagesPublished = messagesPublished.get()
//}