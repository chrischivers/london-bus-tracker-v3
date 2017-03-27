package lbt.database

import akka.actor.Kill
import lbt.StandardTestFixture
import lbt.comon.BusRoute
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.fixture

import scala.concurrent.duration._
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.ExecutionContext

class DefinitionsTest extends fixture.FunSuite with ScalaFutures {

  type FixtureParam = StandardTestFixture
  implicit val executionContext = ExecutionContext.Implicits.global

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(10 seconds),
    interval = scaled(1 second)
  )

  override def withFixture(test: OneArgTest) = {
    val fixture = new StandardTestFixture
    try test(fixture)
    finally {
      fixture.actorSystem.terminate().futureValue
      fixture.testDefinitionsTable.deleteTable
      fixture.testHistoricalTable.deleteTable
      Thread.sleep(1000)
    }
  }

  test("Route definitions for existing bus number 3 can be loaded from DB") { f =>

    val busDefinitions = f.testDefinitionsTable.getBusRouteDefinitions()
    busDefinitions.get(f.testBusRoute1) shouldBe defined
    busDefinitions.get(f.testBusRoute2) shouldBe defined
    busDefinitions(f.testBusRoute1).count(stop => stop.stopName.contains("Brixton")) should be > 0
    busDefinitions(f.testBusRoute2).count(stop => stop.stopName.contains("Brixton")) should be > 0

  }

  test("Bus Route not already in DB is reloaded from web on request") { f =>
    f.testDefinitionsTable.getBusRouteDefinitions()
    val newBusRoute = BusRoute("521", "inbound")
    f.testDefinitionsTable.getBusRouteDefinitions().get(newBusRoute) shouldBe empty
    f.testDefinitionsTable.refreshBusRouteDefinitionFromWeb(updateNewRoutesOnly = true, getOnly = Some(List(newBusRoute)))
    Thread.sleep(5000)
    f.testDefinitionsTable.getBusRouteDefinitions().get(f.testBusRoute1) shouldBe defined
    f.testDefinitionsTable.getBusRouteDefinitions().get(f.testBusRoute2) shouldBe defined
    f.testDefinitionsTable.getBusRouteDefinitions().get(newBusRoute) shouldBe defined
    f.testDefinitionsTable.getBusRouteDefinitions()(newBusRoute).count(stop => stop.stopName.contains("London Bridge")) should be > 0
  }

  test("Sequence is kept in order when loaded from web and retrieved from db") { f =>
    val busDefinitions = f.testDefinitionsTable.getBusRouteDefinitions()
    busDefinitions.get(f.testBusRoute1) shouldBe defined
    busDefinitions(f.testBusRoute1).head.stopName should include("Conduit Street")
    busDefinitions(f.testBusRoute1).last.stopName should include("Crystal Palace")
  }
}
