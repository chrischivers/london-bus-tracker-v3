package lbt.dataSource

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
    val sourceLineBytes = sourceLineToString(sourceLine).getBytes
    producer ! Publish(config.exchangeName, config.historicalRecorderRoutingKey, sourceLineBytes, properties = None, mandatory = true, immediate = false)
    producer ! Publish(config.exchangeName, config.liveTrackerRoutingKey, sourceLineBytes, properties = None, mandatory = true, immediate = false)
  }

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