import historical.HistoricalTestFixture
import lbt.comon.{BusRoute, Stop}
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.fixture
import org.scalatest.time.{Millis, Seconds, Span}

class DefinitionsTest extends fixture.FunSuite with ScalaFutures {


  type FixtureParam = HistoricalTestFixture

  override implicit val patienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(500, Millis)))

  override def withFixture(test: OneArgTest) = {
    val fixture = new HistoricalTestFixture
    try test(fixture)
    finally {
      fixture.dataStreamProcessingControllerReal.stop
      fixture.actorSystem.terminate().futureValue
      fixture.testDefinitionsCollection.db.dropDatabase
    }
  }

  test("Route definitions for bus number 3 can be loaded from web into DB") { f =>

    val testBusRoute = BusRoute("3", "outbound")
    val getOnlyList = List(testBusRoute)

    f.testDefinitionsCollection.refreshBusRouteDefinitionFromWeb(getOnly = Some(getOnlyList))

    eventually {
      val busDefinitions = f.testDefinitionsCollection.getBusRouteDefinitions()
      busDefinitions.get(testBusRoute) shouldBe defined
      busDefinitions(testBusRoute).count(stop => stop.name.contains("Brixton")) should be > 1
    }
  }

  test("Bus Route already in DB is not reloaded from web when flag set") { f =>
      //TODO
  }

  test("Sequence is kept in order when loaded from web and retrieved from db") { f =>
    val testBusRoute = BusRoute("3", "outbound")
    val getOnlyList = List(testBusRoute)
    f.testDefinitionsCollection.refreshBusRouteDefinitionFromWeb(getOnly = Some(getOnlyList))
    eventually {
      val busDefinitions = f.testDefinitionsCollection.getBusRouteDefinitions()
      busDefinitions.get(testBusRoute) shouldBe defined
      busDefinitions(testBusRoute).head.name should include ("Conduit Street")
      busDefinitions(testBusRoute).last.name should include ("Crystal Palace")

    }
  }
}
