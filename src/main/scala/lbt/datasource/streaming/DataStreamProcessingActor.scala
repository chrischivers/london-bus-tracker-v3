package lbt.datasource.streaming

import akka.actor.Actor
import com.typesafe.scalalogging.StrictLogging
import lbt.DataSourceConfig
import lbt.comon.{Start, Stop}
import lbt.datasource.BusDataSource
import lbt.historical.{HistoricalSourceLineProcessor, VehicleActorSupervisor}

/**
 * Actor that iterates over live stream sending lines to be processed. On crash, the supervisor strategy restarts it
 */
class DataStreamProcessingActor(historicalMessageProcessor: HistoricalSourceLineProcessor, dataSourceConfig: DataSourceConfig) extends Actor with StrictLogging {

  logger.info("Data stream Processing Actor Created")
  val dataSource = new BusDataSource(dataSourceConfig)

  // Iterating pattern for this actor based on code snippet posted on StackOverflow
  //http://stackoverflow.com/questions/5626285/pattern-for-interruptible-loops-using-actors
  override def receive: Receive = inactive // Start out as inactive

  def inactive: Receive = { // This is the behavior when inactive
    case Start =>
      logger.info("DataStreamProcessingActor Actor becoming active")
      context.become(active)
      self ! Next
  }

  def active: Receive = { // This is the behavior when it's active
    case Stop =>
      context.become(inactive)
      logger.info("DataStreamProcessingActor becoming inactive")
    case Next =>
     if(dataSource.hasNext){
       historicalMessageProcessor.processSourceLine(dataSource.next())
       context.parent ! Increment
       self ! Next
     } else {
       logger.info("Data source iterator is empty. No line to process. Waiting 500 ms")
       Thread.sleep(500)
       self ! Next
     }

  }

  override def postStop(): Unit = {
    logger.info("Post Stop being called for Data Processing Actor")
    dataSource.closeClient()
    super.postStop()
  }

  override def postRestart(reason: Throwable): Unit = {
    logger.info("Post restart being called for Data Processing Actor")
    self ! Start
  }


}
