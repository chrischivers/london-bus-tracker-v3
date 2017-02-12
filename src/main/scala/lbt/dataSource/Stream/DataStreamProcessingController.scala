package lbt.dataSource.Stream

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorSystem, OneForOneStrategy, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.concurrent.{Await, TimeoutException}

case class Start()
case class Stop()
case class Next()
case class Increment()
case class GetNumberProcessed()

class DataStreamProcessingController extends Actor with StrictLogging {

  val iteratingActor = context.actorOf(Props[DataStreamProcessingActor])
  var numberProcessed:Long = 0
  var numberProcessedSinceRestart:Long = 0

  def receive = {
    case  Start=>
      logger.info("Supervisor starting the iterating actor")
      iteratingActor ! Start
      iteratingActor ! Next
    case Stop =>
      iteratingActor ! Stop
    case Increment => incrementNumberProcessed()
    case GetNumberProcessed => sender ! numberProcessed
  }

  def incrementNumberProcessed() = {
    numberProcessed += 1
    numberProcessedSinceRestart += 1
  }

  /**
   * Supervises the Actor, ensuring that it restarts if it crashes
   */
  override val supervisorStrategy =
    OneForOneStrategy(loggingEnabled = false) {
      case e: TimeoutException =>
        logger.debug("Incoming Stream TimeOut Exception. Restarting...")
        Thread.sleep(5000)
       numberProcessedSinceRestart = 0
        Restart
      case e: Exception =>
        logger.debug("Exception. Incoming Stream Exception. Restarting...")
        e.printStackTrace()
        Thread.sleep(5000)
        numberProcessedSinceRestart = 0
        Restart
      case t =>
        super.supervisorStrategy.decider.applyOrElse(t, (_: Any) => Escalate)
    }

}


object DataStreamProcessingController extends StrictLogging {

  implicit val timeout = Timeout(5 seconds) // needed for `?` below

  implicit val system = ActorSystem("lbtSystem")

  val publisher = new SourceLinePublisher

  val supervisor = system.actorOf(Props[DataStreamProcessingController])

  def start(): Unit = {
    logger.info("Starting Bus Stream Supervisor")
    supervisor ! Start

  }

  def stop(): Unit = {
    logger.info("Stopping Bus Stream Supervisor")
    supervisor ! Stop
  }

  def numberProcessed: Long = {
    val answer = Await.result(supervisor ? GetNumberProcessed, 10 seconds)
    answer.asInstanceOf[Long]
  }

}
