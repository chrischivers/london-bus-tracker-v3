import lbt.ConfigLoader
import lbt.dataSource.BusDataSource
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scalaz.{-\/, \/-}

class BusDataSourceTest extends FunSuite {

  val config = ConfigLoader.defaultConfig.dataSourceConfig

  test("Data stream should be returned") {
    withClue("No data stream returned") {
      BusDataSource.getNewStream(config).isRight shouldBe true
      BusDataSource.getNewStream(config).isLeft shouldBe false
    }
  }

   test("Stream should not be opened without previous being closed") {
     val x = BusDataSource.getNewStream(config).getOrElse(throw new IllegalStateException)
     val y = BusDataSource.getNewStream(config).getOrElse(throw new IllegalStateException)

     println(x.head)


     y.headOption.get.length should be > 0
     x.headOption.isEmpty shouldBe true


   }

}
