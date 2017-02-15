package lbt.dataSource.Stream

import akka.actor.Actor
import com.typesafe.scalalogging.StrictLogging

/**
 * Actor that iterates over live stream sending lines to be processed. On crash, the supervisor strategy restarts it
 */
class DataStreamProcessingActor(dataSource: BusDataSource, publisher: SourceLinePublisher) extends Actor with StrictLogging {

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
     if(dataSource.hasNext){
       publisher.publish(dataSource.next())
       context.parent ! Increment
       self ! Next
     } else logger.info("Data source iterator is empty. No line to process.")

  }

  override def postRestart(reason: Throwable): Unit = {
    logger.debug("DataStreamProcessingActor Restarting")
    dataSource.reloadDataSource()
    self ! Start
    self ! Next
  }


}
