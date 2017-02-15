import lbt.comon.{BusRoute, Outbound}
import lbt.dataSource.definitions.BusDefinitionsOps
import lbt.{ConfigLoader, DefinitionsConfig}
import lbt.database.{DbCollections, MongoDatabase}
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfter, FunSuite}

class DefinitionsTest extends FunSuite with BeforeAndAfter {

  implicit val patienceConfig =  PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(1000, Millis)))

  val testDBConfig = ConfigLoader.defaultConfig.databaseConfig.copy(databaseName = "TestDB")
  val dbCol = DbCollections.loadBusDefinitionsCollection(dbConfig = testDBConfig)

  before {dbCol.db.dropDatabase}

  after {dbCol.db.dropDatabase}

  test("Route definitions for bus number 3 can be loaded from web into DB") {

    val testBusRoute = BusRoute("3", Outbound())
    val getOnlyList = List(testBusRoute)
    BusDefinitionsOps.loadBusDefinitionsOps(dbCol).refreshBusRouteDefinitionFromWeb(getOnly = Some(getOnlyList))
    eventually {
      val busDefinitions = BusDefinitionsOps.loadBusDefinitionsOps(dbCol)
      busDefinitions.busRouteDefinitions.get(testBusRoute) shouldBe defined
      busDefinitions.busRouteDefinitions(testBusRoute).count(stop => stop.name.contains("Brixton")) should be > 1
    }
  }

  test("Bus Route  already in DB is not reloaded from web when flag set") {

    val testBusRoute = BusRoute("3", Outbound())
    val getOnlyList = List(testBusRoute)
    BusDefinitionsOps.loadBusDefinitionsOps(dbCol)
      .refreshBusRouteDefinitionFromWeb(updateNewRoutesOnly = true, getOnly = Some(getOnlyList))

    // Fake url would throw error if called
    val testDefConfig = ConfigLoader.defaultConfig.definitionsConfig.copy(sourceSingleUrl = "fake#RouteID#fake#Direction#")
    val dbCol2 = DbCollections.loadBusDefinitionsCollection(dbConfig = testDBConfig, defConfig = testDefConfig)
    BusDefinitionsOps.loadBusDefinitionsOps(dbCol2)
      .refreshBusRouteDefinitionFromWeb(updateNewRoutesOnly = true, getOnly = Some(getOnlyList))

    eventually {
      val busDefinitions = BusDefinitionsOps.loadBusDefinitionsOps(dbCol)
      busDefinitions.busRouteDefinitions.get(testBusRoute) shouldBe defined
      busDefinitions.busRouteDefinitions(testBusRoute).count(stop => stop.name.contains("Brixton")) should be > 1
    }
  }

  test("Sequence is kept in order when loaded from web and retrieved from db") {
    val testBusRoute = BusRoute("3", Outbound())
    val getOnlyList = List(testBusRoute)
    BusDefinitionsOps.loadBusDefinitionsOps(dbCol).refreshBusRouteDefinitionFromWeb(getOnly = Some(getOnlyList))
    eventually {
      val busDefinitions = BusDefinitionsOps.loadBusDefinitionsOps(dbCol)
      busDefinitions.busRouteDefinitions.get(testBusRoute) shouldBe defined
      busDefinitions.busRouteDefinitions(testBusRoute).head.name should include ("Conduit Street")
      busDefinitions.busRouteDefinitions(testBusRoute).last.name should include ("Crystal Palace")

    }
  }
}
