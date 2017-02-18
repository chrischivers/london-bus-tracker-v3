package lbt.dataSource.Stream

import java.util.concurrent.atomic.AtomicLong

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import lbt.dataSource.Stream.BusDataSource.BusDataSource
import lbt.{ConfigLoader, MessagingConfig}

import scala.concurrent.duration._
import scala.concurrent.{Await, TimeoutException}
import scala.util.Random

case class Start()
case class Stop()
case class Next()
case class Increment()
case class GetNumberLinesProcessed()
case class GetNumberLinesProcessedSinceRestart()

class DataStreamProcessingController(dataSource: BusDataSource, config: MessagingConfig) extends Actor with StrictLogging {

  val publisher = new SourceLinePublisher(config)
  val iteratingActor: ActorRef = context.actorOf(Props(classOf[DataStreamProcessingActor], dataSource, publisher))

  var numberProcessed = new AtomicLong(0)
  var numberProcessedSinceRestart = new AtomicLong(0)

  implicit val timeout = Timeout(5 seconds) // needed for `?` below

  def receive = {
    case  Start=>
      logger.info("Supervisor starting the iterating actor")
      iteratingActor ! Start
      iteratingActor ! Next
    case Stop =>
      logger.info("Supervisor stopping the iterating actor")
      iteratingActor ! Stop
    case Increment => incrementNumberProcessed()
    case GetNumberLinesProcessed => sender ! numberProcessed.get()
    case GetNumberLinesProcessedSinceRestart => sender ! numberProcessedSinceRestart.get()
  }

  def incrementNumberProcessed() = {
    numberProcessed.incrementAndGet()
    numberProcessedSinceRestart.incrementAndGet()
  }

  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    logger.info("Post restart method of actor")
    context.stop(iteratingActor)
    dataSource.closeClient()
    super.postStop()
  }

  /**
   * Supervises the Actor, ensuring that it restarts if it crashes
   */
  override val supervisorStrategy =
    OneForOneStrategy(loggingEnabled = false) {
      case e: TimeoutException =>
        logger.debug("Incoming Stream TimeOut Exception. Restarting...")
        Thread.sleep(5000)
       numberProcessedSinceRestart.set(0)
        Restart
      case e: Exception =>
        logger.debug("Exception. Incoming Stream Exception. Restarting...")
        e.printStackTrace()
        Thread.sleep(5000)
        numberProcessedSinceRestart.set(0)
        Restart
      case t =>
        super.supervisorStrategy.decider.applyOrElse(t, (_: Any) => Escalate)
    }

}

object DataStreamProcessingController {
  implicit val system = ActorSystem("lbtSystem")
  val defaultMessagingConfig = ConfigLoader.defaultConfig.messagingConfig
  val defaultDataSourceConfig = ConfigLoader.defaultConfig.dataSourceConfig

  def apply(): ActorRef = apply(defaultMessagingConfig)

  def apply(config: MessagingConfig): ActorRef = apply(new BusDataSource(defaultDataSourceConfig), config)

  def apply(dataSource: BusDataSource, config: MessagingConfig) =
    system.actorOf(Props(classOf[DataStreamProcessingController], dataSource, config))
}
