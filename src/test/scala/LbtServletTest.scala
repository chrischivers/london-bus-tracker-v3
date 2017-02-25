import lbt.comon.Stop
import lbt.servlet.LbtServlet
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.fixture
import org.scalatra.test.scalatest.{ScalatraFunSuite, ScalatraSuite}

import scala.concurrent.duration._


class LbtServletTest extends fixture.FunSuite with ScalaFutures with Eventually with ScalatraSuite {

  addServlet(classOf[LbtServlet], "/*")
  type FixtureParam = TestFixture

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(30 seconds),
    interval = scaled(1 second)
  )
  override def withFixture(test: OneArgTest) = {
    val fixture = new TestFixture
    try test(fixture)
    finally {
      fixture.dataStreamProcessingControllerReal ! Stop
      fixture.actorSystem.terminate().futureValue
      fixture.consumer.unbindAndDelete
      fixture.testDefinitionsCollection.db.dropDatabase
    }
  }

  test("endpoint test") {f=>
      get("/") {

      }
  }

}
