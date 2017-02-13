package lbt.dataSource.Stream

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import lbt.{ConfigLoader, MessagingConfig}

import scala.concurrent.duration._
import scala.concurrent.{Await, TimeoutException}

case class Start()
case class Stop()
case class Next()
case class Increment()
case class GetNumberProcessed()

class DataStreamProcessingController(dataSource: BusDataSource, config: MessagingConfig) extends Actor with StrictLogging {

  val publisher = new SourceLinePublisher(config)
  val iteratingActor: ActorRef = context.actorOf(Props(new DataStreamProcessingActor(dataSource, publisher)))

  var numberProcessed:Long = 0
  var numberProcessedSinceRestart:Long = 0

  implicit val timeout = Timeout(5 seconds) // needed for `?` below

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

object DataStreamProcessingController {
  implicit val system = ActorSystem("lbtSystem")
  val defaultMessagingConfig = ConfigLoader.defaultConfig.messagingConfig
  val defaultDataSourceConfig = ConfigLoader.defaultConfig.dataSourceConfig

  def apply(): ActorRef = apply(defaultMessagingConfig)

  def apply(config: MessagingConfig): ActorRef = apply(BusDataSource(defaultDataSourceConfig), config)

  def apply(dataSource: BusDataSource, config: MessagingConfig) =
    system.actorOf(Props(
    new DataStreamProcessingController(dataSource, config)))
}
