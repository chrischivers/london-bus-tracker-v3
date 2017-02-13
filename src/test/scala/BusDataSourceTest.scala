import lbt.ConfigLoader
import lbt.dataSource.Stream.BusDataSource
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scalaz.{-\/, \/-}

class BusDataSourceTest extends FunSuite {

  val testDataSourceConfig = ConfigLoader.defaultConfig.dataSourceConfig

  test("Data stream should be opened and return with next value") {
    withClue("No data stream returned") {
      BusDataSource(testDataSourceConfig).hasNext shouldBe true
    }
  }

   test("Iterator should reload when required") {

     val dataSource = BusDataSource(testDataSourceConfig)
     dataSource.hasNext shouldBe true

     dataSource.reloadDataSource()

     val reloadedDataSource = BusDataSource(testDataSourceConfig)
     reloadedDataSource.hasNext shouldBe true
     reloadedDataSource.hashCode() shouldNot equal(dataSource)
   }

  test("Iterator should not create new instance when requested again") {

    val dataSource = BusDataSource(testDataSourceConfig)
    dataSource.hasNext shouldBe true

    val newDataSource = BusDataSource(testDataSourceConfig)
    newDataSource.hasNext shouldBe true
    newDataSource.hashCode() shouldEqual(dataSource)
  }
}
