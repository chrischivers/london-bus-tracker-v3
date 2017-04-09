package lbt.historical

import akka.actor.Kill
import lbt.StandardTestFixture
import lbt.datasource.SourceLine
import lbt.datasource.streaming.DataStreamProcessor
import org.scalatest.Matchers._
import org.scalatest._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._
import scala.util.Random

class HistoricalRecorderProcessTest extends fixture.FunSuite with ScalaFutures with Eventually {

  type FixtureParam = StandardTestFixture

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(30 seconds),
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

  test("Line should be accepted if route is in definitions or disregarded if not in definitions") { f =>

    val routeDefFromDb = f.definitions(f.testBusRoute1)
    def randomStop = routeDefFromDb(Random.nextInt(routeDefFromDb.size - 1))

    val validSourceline = "[1,\"" + randomStop.stopID + "\",\"" + f.testBusRoute1.name + "\",1,\"Any Place\",\"SampleReg\"," + f.generateArrivalTime + "]"
    val invalidSourceLine = "[1,\"" + randomStop.stopID + "\",\"99XXXX\",2,\"Bromley North\",\"YX62DYN\"," + f.generateArrivalTime + "]"
    val testLines = List(validSourceline, invalidSourceLine)

    val testDataSourceConfig = f.testDataSourceConfig.copy(simulationIterator = Some(testLines.toIterator))
    val dataStreamProcessorTest = new DataStreamProcessor(testDataSourceConfig, f.historicalSourceLineProcessor)(f.actorSystem, f.executionContext)

    dataStreamProcessorTest.start
    eventually {
      dataStreamProcessorTest.numberLinesProcessed.futureValue shouldBe 2
      f.historicalSourceLineProcessor.numberSourceLinesProcessed.get() shouldBe 2
      f.historicalSourceLineProcessor.numberSourceLinesValidated.get() shouldBe 1
    }
    dataStreamProcessorTest.stop
  }

  test("Line should be accepted if bus stop is in definitions and rejected if not") { f =>

    val routeDefFromDb = f.definitions(f.testBusRoute1)
    def randomStop = routeDefFromDb(Random.nextInt(routeDefFromDb.size - 1))

    val validSourceline = "[1,\"" + randomStop.stopID + "\",\"" + f.testBusRoute1.name + "\",1,\"Any Place\",\"SampleReg\"," + f.generateArrivalTime  + "]"
    val invalidSourceLine = "[1,\"" + "XXSTOP" + "\",\"" + f.testBusRoute1.name + "\",1,\"Any Place\",\"SampleReg\"," + f.generateArrivalTime  + "]"
    val testLines = List(validSourceline, invalidSourceLine)

    val testConfig = f.testDataSourceConfig.copy(simulationIterator = Some(testLines.toIterator))
    val dataStreamProcessorTest = new DataStreamProcessor(testConfig, f.historicalSourceLineProcessor)(f.actorSystem, f.executionContext)

    dataStreamProcessorTest.start

    eventually {
      dataStreamProcessorTest.numberLinesProcessed.futureValue shouldBe 2
      f.historicalSourceLineProcessor.numberSourceLinesProcessed.get() shouldBe 2
      f.historicalSourceLineProcessor.numberSourceLinesValidated.get() shouldBe 1
    }
    dataStreamProcessorTest.stop
  }

  test("Lines received with arrival time in past should be ignored"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute1)

    val testLines: List[String] = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.stopID + "\",\"" + f.testBusRoute1.name + "\",1,\"Any Place\",\"V123456\"," + f.generateArrivalTime + "]")

    val splitLines = testLines.splitAt(testLines.size / 2)
    val pastLine = List(splitLines._2.head.replace(splitLines._2.head.substring(splitLines._2.head.lastIndexOf(",") + 1, splitLines._2.head.lastIndexOf("]")), (System.currentTimeMillis() - 100000000).toString))
    val joinedLines = splitLines._1 ++ pastLine ++ splitLines._2.tail

    val testConfig = f.testDataSourceConfig.copy(simulationIterator = Some(joinedLines.toIterator))
    val dataStreamProcessorTest = new DataStreamProcessor(testConfig, f.historicalSourceLineProcessor)(f.actorSystem, f.executionContext)

    dataStreamProcessorTest.start

    eventually {
      dataStreamProcessorTest.numberLinesProcessed.futureValue shouldBe joinedLines.size
      f.historicalSourceLineProcessor.numberSourceLinesProcessed.get() shouldBe joinedLines.size
      f.historicalSourceLineProcessor.numberSourceLinesValidated.get() shouldBe joinedLines.size - 1
      f.vehicleActorSupervisor.getCurrentActors.futureValue.size shouldBe 1
    }
    dataStreamProcessorTest.stop
  }



  test("An actor should be created for each new vehicle received with a new route") { f=>

    val routeDef1FromDb = f.definitions(f.testBusRoute1)
    val routeDef2FromDb = f.definitions(f.testBusRoute2)
    def randomStopRoute1 = routeDef1FromDb(Random.nextInt(routeDef1FromDb.size - 1))
    def randomStopRoute2 = routeDef2FromDb(Random.nextInt(routeDef2FromDb.size - 1))

    val validSourceline1 = "[1,\"" + randomStopRoute1.stopID + "\",\"" + f.testBusRoute1.name + "\",1,\"Any Place\",\"V123456\"," + f.generateArrivalTime + "]"
    val validSourceline2 = "[1,\"" + randomStopRoute1.stopID + "\",\"" + f.testBusRoute1.name + "\",1,\"Any Place\",\"V123456\"," + f.generateArrivalTime + "]"
    val validSourceline3 = "[1,\"" + randomStopRoute1.stopID + "\",\"" + f.testBusRoute1.name + "\",1,\"Any Place\",\"V23456\"," + f.generateArrivalTime + "]"
    val validSourceline4 = "[1,\"" + randomStopRoute2.stopID + "\",\"" + f.testBusRoute2.name + "\",2,\"Any Place\",\"V23456\"," + f.generateArrivalTime + "]"

    val testLines = List(validSourceline1, validSourceline2, validSourceline3, validSourceline4)

    val testConfig = f.testDataSourceConfig.copy(simulationIterator = Some(testLines.toIterator))
    val dataStreamProcessorTest = new DataStreamProcessor(testConfig, f.historicalSourceLineProcessor)(f.actorSystem, f.executionContext)

    dataStreamProcessorTest.start

    eventually {
      dataStreamProcessorTest.numberLinesProcessed.futureValue shouldBe 4
      f.historicalSourceLineProcessor.numberSourceLinesProcessed.get() shouldBe 4
      f.historicalSourceLineProcessor.numberSourceLinesValidated.get() shouldBe 4
      f.vehicleActorSupervisor.getCurrentActors.futureValue.size shouldBe 3
    }
    dataStreamProcessorTest.stop
  }

  test("Vehicle actors should add incoming lines relating to arrival times to stop record"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute1)

    val testLines = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.stopID + "\",\"" + f.testBusRoute1.name + "\",1,\"Any Place\",\"V123456\"," + f.generateArrivalTime + "]")

    val testConfig = f.testDataSourceConfig.copy(simulationIterator = Some(testLines.toIterator))
    val dataStreamProcessorTest = new DataStreamProcessor(testConfig, f.historicalSourceLineProcessor)(f.actorSystem, f.executionContext)

    dataStreamProcessorTest.start

    eventually {
      dataStreamProcessorTest.numberLinesProcessed.futureValue shouldBe testLines.size
      f.historicalSourceLineProcessor.numberSourceLinesProcessed.get() shouldBe testLines.size
      f.historicalSourceLineProcessor.numberSourceLinesValidated.get() shouldBe testLines.size
      f.vehicleActorSupervisor.getCurrentActors.futureValue.size shouldBe 1
      f.vehicleActorSupervisor.getLiveArrivalRecords(VehicleActorID("V123456", f.testBusRoute1)).futureValue.size shouldBe testLines.size
    }
    dataStreamProcessorTest.stop
  }


  test("Vehicle actors should update record when closer arrival time is received"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute1)
    val arrivalTime = System.currentTimeMillis() + 100000

    val testLines = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.stopID + "\",\"" + f.testBusRoute1.name + "\",1,\"Any Place\",\"V123456\"," + arrivalTime + "]")

    val newArrivalTime = arrivalTime - 50000
    val newRecord = testLines.head.replace(arrivalTime.toString, newArrivalTime.toString)
    val testLinesWithUpdated = testLines ++ List(newRecord)

    val testConfig = f.testDataSourceConfig.copy(simulationIterator = Some(testLinesWithUpdated.toIterator))
    val dataStreamProcessorTest = new DataStreamProcessor(testConfig, f.historicalSourceLineProcessor)(f.actorSystem, f.executionContext)

    dataStreamProcessorTest.start

    eventually {
      dataStreamProcessorTest.numberLinesProcessed.futureValue shouldBe testLinesWithUpdated.size
      f.historicalSourceLineProcessor.numberSourceLinesProcessed.get() shouldBe testLinesWithUpdated.size
      f.historicalSourceLineProcessor.numberSourceLinesValidated.get() shouldBe testLinesWithUpdated.size
      f.vehicleActorSupervisor.getCurrentActors.futureValue.size shouldBe 1
      f.vehicleActorSupervisor.getLiveArrivalRecords(VehicleActorID("V123456", f.testBusRoute1)).futureValue.apply(routeDefFromDb.head) shouldBe newArrivalTime
    }
    dataStreamProcessorTest.stop
  }

}
