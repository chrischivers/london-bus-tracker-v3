package lbt.datasource.streaming

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, Props}
import akka.util.Timeout
import akka.pattern.ask
import com.typesafe.scalalogging.StrictLogging
import lbt.comon.{Start, Stop}
import lbt.datasource.BusDataSource
import lbt.historical.HistoricalSourceLineProcessor
import lbt.{ConfigLoader, DataSourceConfig, MessagingConfig}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.concurrent.duration._

case class Next()
case class Increment()
case class GetNumberLinesProcessed()
case class GetNumberLinesProcessedSinceRestart()
case class GetTimeOfLastRestart()
case class GetNumberOfRestarts()

class DataStreamProcessingController(dataSourceConfig: DataSourceConfig, messagingConfig: MessagingConfig, historicalSourceLineProcessor: HistoricalSourceLineProcessor) extends Actor with StrictLogging {
  logger.info("Data stream Processing Controller Actor Created")

//  val publisher = new SourceLinePublisher(messagingConfig)(context.system)
  val iteratingActor: ActorRef = context.actorOf(Props(classOf[DataStreamProcessingActor], historicalSourceLineProcessor, dataSourceConfig), "dataStreamProcessingActor")

  val numberProcessed = new AtomicLong(0)
  val numberProcessedSinceRestart = new AtomicLong(0)
  val timeOfLastRestart = new AtomicLong(System.currentTimeMillis())
  val numberOfRestarts = new AtomicInteger(0)

  implicit val timeout = Timeout(5 seconds) // needed for `?` below

  def receive = {
    case  Start=>
      logger.info("Supervisor starting the iterating actor")
      iteratingActor ! Start
    case Stop =>
      logger.info("Supervisor stopping the iterating actor")
      iteratingActor ! Stop
    case Increment => incrementNumberProcessed()
    case GetNumberLinesProcessed => sender ! numberProcessed.get()
    case GetNumberLinesProcessedSinceRestart => sender ! numberProcessedSinceRestart.get()
    case GetTimeOfLastRestart => sender ! timeOfLastRestart.get()
    case GetNumberOfRestarts => sender ! numberOfRestarts.get()
  }

  def incrementNumberProcessed() = {
    numberProcessed.incrementAndGet()
    numberProcessedSinceRestart.incrementAndGet()
  }

  /**
   * Supervises the Actor, ensuring that it restarts if it crashes
   */
  override val supervisorStrategy =
    OneForOneStrategy(loggingEnabled = false) {
      case e: TimeoutException =>
        logger.error("Incoming Stream TimeOut Exception. Restarting...", e)
        Thread.sleep(5000)
       numberProcessedSinceRestart.set(0)
        timeOfLastRestart.set(System.currentTimeMillis())
        numberOfRestarts.incrementAndGet()
        Restart
      case e: Exception =>
        logger.error("Exception. Incoming Stream Exception. Restarting...", e)
        Thread.sleep(5000)
        numberProcessedSinceRestart.set(0)
        timeOfLastRestart.set(System.currentTimeMillis())
        numberOfRestarts.incrementAndGet()
        Restart
      case t =>
        super.supervisorStrategy.decider.applyOrElse(t, (_: Any) => Escalate)
    }

}

class DataStreamProcessor(dataSourceConfig : DataSourceConfig, messagingConfig: MessagingConfig, historicalSourceLineProcessor: HistoricalSourceLineProcessor)(implicit actorSystem: ActorSystem, ec: ExecutionContext)  {
  implicit val timeout:Timeout = 20 seconds
  val dtf: DateTimeFormatter = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss")
  val processorControllerActor = actorSystem.actorOf(Props(classOf[DataStreamProcessingController], dataSourceConfig, messagingConfig, historicalSourceLineProcessor), "dataStreamProcessingController")

  def start = processorControllerActor ! Start

  def stop = processorControllerActor ! Stop

  def numberLinesProcessed: Future[Long] = {
    (processorControllerActor ? GetNumberLinesProcessed).mapTo[Long]
  }

  def numberLinesProcessedSinceRestart: Future[Long] = {
    (processorControllerActor ? GetNumberLinesProcessedSinceRestart).mapTo[Long]
  }

  def numberOfRestarts: Future[Int] = {
    (processorControllerActor ? GetNumberOfRestarts).mapTo[Int]
  }

  def timeOfLastRestart: Future[String] = {
    for {
      timeOfRestart <- (processorControllerActor ? GetTimeOfLastRestart).mapTo[Long]
      formattedDate = dtf.print(timeOfRestart)
    } yield formattedDate
  }
}
