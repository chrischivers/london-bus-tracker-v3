package lbt.historical

import lbt.comon.{BusRoute, Commons}
import lbt.dataSource.Stream.SourceLine
import lbt.dataSource.definitions.BusDefinitionsOps
import lbt.database.DbCollections
import lbt.{ConfigLoader, MessageConsumer, MessageProcessor, MessagingConfig}
import net.liftweb.json.{DefaultFormats, parse}
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future


object HistoricalRecorder {
  val defaultMessagingConfig = ConfigLoader.defaultConfig.messagingConfig
  val defaultHistoricalMessageProcessor = new HistoricalMessageProcessor

  def apply(): MessageConsumer = apply(defaultHistoricalMessageProcessor, defaultMessagingConfig)
  def apply(historicalMessageProcessor: HistoricalMessageProcessor, messagingConfig: MessagingConfig) =
    new MessageConsumer(historicalMessageProcessor, messagingConfig.exchangeName, messagingConfig.historicalRecorderQueueName, messagingConfig.historicalRecorderRoutingKey)
}

class HistoricalMessageProcessor extends MessageProcessor {

  var lastProcessedMessage: Option[SourceLine] = None
  var messagesProcessed: Long = 0
  var lastValidatedMessage: Option[SourceLine] = None
  var messagesValidated: Long = 0

  implicit val formats = DefaultFormats
  override def apply(message: Array[Byte]): Boolean = {

    val sourceLine = parse(new String(message, "UTF-8")).extract[SourceLine]
    println(s"message received: $sourceLine")
    lastProcessedMessage = Some(sourceLine)
    messagesProcessed += 1
    for {
      _ <- routeIsDefined(sourceLine)
    } yield {
      lastValidatedMessage = Some(sourceLine)
      messagesValidated += 1
    }

    true
  }

  def routeIsDefined(sourceLine: SourceLine): Future[Unit] = {
    Future {
      val busRoute = BusRoute(sourceLine.route, Commons.toDirection(sourceLine.direction))
      val definitions = BusDefinitionsOps.loadBusDefinitionsOps()
      definitions.busRouteDefinitions.get(busRoute) match {
        case Some(_) => Future.successful()
        case None => Future.failed(throw new IllegalArgumentException(s"Route $busRoute not defined in definitions"))
      }
    }
  }

  def notOnIgnoreList: Boolean = true
}







