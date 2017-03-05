package datasource

import lbt.ConfigLoader
import lbt.datasource.BusDataSource
import lbt.datasource.streaming.DataStreamProcessor
import org.apache.http.TruncatedChunkException
import org.scalatest.{FunSuite, Matchers}

class BusDataSourceTest extends FunSuite with Matchers {

  val testDataSourceConfig = ConfigLoader.defaultConfig.dataSourceConfig
  val testMessagingConfig = ConfigLoader.defaultConfig.messagingConfig.copy(
    exchangeName = "test-lbt-exchange",
    historicalRecorderQueueName = "test-historical-recorder-queue-name",
    historicalRecorderRoutingKey = "test-historical-recorder-routing-key")

//  test("Data stream should be opened and return with next value") {
//    withClue("No data stream returned") {
//      val dataStreamProcessorTest = new DataStreamProcessor(testDataSourceConfig, testMessagingConfig)
//      dataStreamProcessorTest.BusDataSource(testDataSourceConfig).hasNext shouldBe true
//    }
//  }
//
//  test("Iterator should close previous instance when new instance is created") {
//
//    val dataSource = BusDataSource(testDataSourceConfig)
//    dataSource.hasNext shouldBe true
//
//    val newDataSource = BusDataSource(testDataSourceConfig)
//    newDataSource.hasNext shouldBe true
//
//    withClue("checks that iterating over the old data source (which has been closed) results in an error") {
//      assertThrows[TruncatedChunkException] {
//        for (_ <- 0 to 100) {
//          dataSource.next()
//        }
//      }
//    }
//  }
}
