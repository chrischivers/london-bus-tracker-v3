import lbt.ConfigLoader
import lbt.dataSource.Stream.BusDataSource
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scalaz.{-\/, \/-}

class BusDataSourceTest extends FunSuite {

  test("Data stream should be returned with next value") {
    withClue("No data stream returned") {
      BusDataSource.hasNext shouldBe true
    }
  }

   test("Iterator should reload when required") {

     BusDataSource.hasNext shouldBe true
     BusDataSource.reloadIterator()
     BusDataSource.hasNext shouldBe true
   }

  test("Iterator should return a response in the expected format") {
    val line = BusDataSource.next()
      line.route.length should be > 1
  }
}
