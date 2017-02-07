package lbt.dataSource

import akka.actor.Actor
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.{Await, Future}

/**
 * Actor that iterates over live stream sending lines to be processed. On crash, the supervisor strategy restarts it
 */
class DataStreamProcessingActor extends Actor with StrictLogging {

  // Iterating pattern for this actor based on code snippet posted on StackOverflow
  //http://stackoverflow.com/questions/5626285/pattern-for-interruptible-loops-using-actors
  override def receive: Receive = inactive // Start out as inactive

  def inactive: Receive = { // This is the behavior when inactive
    case Start =>
      logger.info("DataStreamProcessingActor Actor becoming active")
      context.become(active)
  }

  def active: Receive = { // This is the behavior when it's active
    case Stop =>
      context.become(inactive)
      logger.info("DataStreamProcessingActor becoming inactive")
    case Next =>
      DataStreamProcessingController.publisher.publish(BusDataSource.next())
      context.parent ! Increment
      self ! Next
  }

  override def postRestart(reason: Throwable): Unit = {
    logger.debug("DataStreamProcessingActor Restarting")
    BusDataSource.reloadIterator()
    self ! Start
    self ! Next
  }


}
