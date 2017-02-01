import lbt.dataSource.{SourceLine, BusDataSource}
import lbt.messaging.RabbitMQ
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class MessagingTest extends FunSuite {

  test("Message should be placed on messaging queue and fetched by consumer") {
    val sourceLine = new SourceLine("3", "outbound", "1000", "Brixton", "VHKDJ2D", System.currentTimeMillis())
      RabbitMQ.publishMessage(sourceLine)
  }

}
