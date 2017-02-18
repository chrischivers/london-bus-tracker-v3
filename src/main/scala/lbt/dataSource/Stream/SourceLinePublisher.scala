package lbt.dataSource.Stream

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.github.sstone.amqp.Amqp._
import com.github.sstone.amqp.{ChannelOwner, ConnectionOwner}
import lbt.{MessagingConfig, RabbitMQConfig}
import net.liftweb.json._
import net.liftweb.json.Serialization.write

import scala.concurrent.duration._

class SourceLinePublisher(config: MessagingConfig) extends RabbitMQConfig  {

  val conn = system.actorOf(ConnectionOwner.props(connFactory, 1 second))
  val producer = ConnectionOwner.createChildActor(conn, ChannelOwner.props())

  val messagesPublished = new AtomicLong(0)

  waitForConnection(system, conn, producer).await(10, TimeUnit.SECONDS)

  producer ! DeclareExchange(ExchangeParameters(config.exchangeName, passive = true, "direct"))

  def publish (sourceLine: SourceLine) = {
    val sourceLineBytes = sourceLineToJson(sourceLine).getBytes
    producer ! Publish(config.exchangeName, config.historicalRecorderRoutingKey, sourceLineBytes, properties = None, mandatory = true, immediate = false)
    messagesPublished.incrementAndGet()
    //producer ! Publish(config.exchangeName, config.liveTrackerRoutingKey, sourceLineBytes, properties = None, mandatory = true, immediate = false)
  }

  def sourceLineToJson(sourceLine: SourceLine): String = {
    implicit val formats = DefaultFormats
    write(sourceLine)
    //sourceLine.toString
    //route: String, direction: String, stopID: String, destinationText: String, vehicleID: String, arrival_TimeStamp: Long)
  }

  def getNumberMessagesPublished = messagesPublished.get()
}