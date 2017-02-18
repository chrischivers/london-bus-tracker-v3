import lbt.ConfigLoader
import lbt.comon.{BusRoute, Outbound}
import lbt.dataSource.definitions.BusDefinitionsOps
import lbt.database.definitions.BusDefinitionsCollection
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfter, FunSuite, fixture}
import scala.concurrent.duration._

class DefinitionsTest extends fixture.FunSuite with ScalaFutures {


  type FixtureParam = TestFixture

  override implicit val patienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(500, Millis)))

  override def withFixture(test: OneArgTest) = {
    val fixture = new TestFixture
    try test(fixture)
    finally {
      fixture.consumer.unbindAndDelete
      fixture.testDefinitionsCollection.db.dropDatabase
    }
  }

  test("Route definitions for bus number 3 can be loaded from web into DB") { f =>

    val testBusRoute = BusRoute("3", Outbound())
    val getOnlyList = List(testBusRoute)

    new BusDefinitionsOps(f.testDefinitionsCollection).refreshBusRouteDefinitionFromWeb(getOnly = Some(getOnlyList))

    eventually {
      val busDefinitions = f.testDefinitionsCollection.getBusRouteDefinitionsFromDB
      busDefinitions.get(testBusRoute) shouldBe defined
      busDefinitions(testBusRoute).count(stop => stop.name.contains("Brixton")) should be > 1
    }
  }

  test("Bus Route already in DB is not reloaded from web when flag set") { f =>
      //TODO
  }

  test("Sequence is kept in order when loaded from web and retrieved from db") { f =>
    val testBusRoute = BusRoute("3", Outbound())
    val getOnlyList = List(testBusRoute)
    new BusDefinitionsOps(f.testDefinitionsCollection).refreshBusRouteDefinitionFromWeb(getOnly = Some(getOnlyList))
    eventually {
      val busDefinitions = f.testDefinitionsCollection.getBusRouteDefinitionsFromDB
      busDefinitions.get(testBusRoute) shouldBe defined
      busDefinitions(testBusRoute).head.name should include ("Conduit Street")
      busDefinitions(testBusRoute).last.name should include ("Crystal Palace")

    }
  }
}
