package lbt.dataSource.Stream

import java.util.concurrent.TimeUnit

import com.github.sstone.amqp.Amqp._
import com.github.sstone.amqp.{ChannelOwner, ConnectionOwner}
import lbt.RabbitMQ

import scala.concurrent.duration._

class SourceLinePublisher extends RabbitMQ  {

  val conn = system.actorOf(ConnectionOwner.props(connFactory, 1 second))
  val producer = ConnectionOwner.createChildActor(conn, ChannelOwner.props())

  waitForConnection(system, conn, producer).await(10, TimeUnit.SECONDS)

  producer ! DeclareExchange(ExchangeParameters(config.exchangeName, passive = true, "direct"))

  def publish (sourceLine: SourceLine) = {
    val sourceLineBytes = sourceLineToJson(sourceLine).getBytes
    producer ! Publish(config.exchangeName, config.historicalRecorderRoutingKey, sourceLineBytes, properties = None, mandatory = true, immediate = false)
    //producer ! Publish(config.exchangeName, config.liveTrackerRoutingKey, sourceLineBytes, properties = None, mandatory = true, immediate = false)
  }

  def sourceLineToJson(sourceLine: SourceLine): String = {
    //write(sourceLine)
    sourceLine.toString
    //route: String, direction: String, stopID: String, destinationText: String, vehicleID: String, arrival_TimeStamp: Long)
  }
}