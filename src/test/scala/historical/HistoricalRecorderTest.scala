package historical

import lbt.comon._
import lbt.datasource.SourceLine
import lbt.datasource.streaming.{DataStreamProcessingController, DataStreamProcessor}
import lbt.historical.{PersistAndRemoveInactiveVehicles, PersistToDB, ValidatedSourceLine, VehicleID}
import org.scalatest.Matchers._
import org.scalatest._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._
import scala.util.Random

class HistoricalRecorderTest extends fixture.FunSuite with ScalaFutures with Eventually {

  type FixtureParam = HistoricalTestFixture

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(30 seconds),
    interval = scaled(1 second)
  )

  override def withFixture(test: OneArgTest) = {
    val fixture = new HistoricalTestFixture
    try test(fixture)
    finally {
      fixture.dataStreamProcessingControllerReal.stop
      fixture.actorSystem.terminate().futureValue
      fixture.testDefinitionsCollection.db.dropDatabase
      fixture.testHistoricalRecordsCollection.db.dropDatabase
    }
  }

  test("Line should be accepted if route is in definitions or disregarded if not in definitions") { f =>

    val routeDefFromDb = f.definitions(f.testBusRoute1)
    def randomStop = routeDefFromDb(Random.nextInt(routeDefFromDb.size - 1))

    val validSourceline = "[1,\"" + randomStop.id + "\",\"" + f.testBusRoute1.id + "\",1,\"Any Place\",\"SampleReg\"," + f.generateArrivalTime + "]"
    val invalidSourceLine = "[1,\"" + randomStop.id + "\",\"99XXXX\",2,\"Bromley North\",\"YX62DYN\"," + f.generateArrivalTime + "]"
    val testLines = List(validSourceline, invalidSourceLine)

    val testConfig = f.testDataSourceConfig.copy(simulationIterator = Some(testLines.toIterator))
    val dataStreamProcessorTest = new DataStreamProcessor(testConfig, f.testMessagingConfig, f.historicalSourceLineProcessor)(f.actorSystem)

    dataStreamProcessorTest.start
    Thread.sleep(500)
    dataStreamProcessorTest.stop

    eventually {
      dataStreamProcessorTest.numberLinesProcessed shouldBe 2
      f.historicalSourceLineProcessor.getNumberProcessed shouldBe 2
      f.historicalSourceLineProcessor.getNumberValidated shouldBe 1
      testLines should contain(sourceLineBackToLine(f.historicalSourceLineProcessor.lastProcessedSourceLine.get))
      validSourceline shouldEqual validatedSourceLineBackToLine(f.historicalSourceLineProcessor.lastValidatedSourceLine.get)
    }
  }

  test("Line should be accepted if bus stop is in definitions and rejected if not") { f =>

    val routeDefFromDb = f.definitions(f.testBusRoute1)
    def randomStop = routeDefFromDb(Random.nextInt(routeDefFromDb.size - 1))


    val validSourceline = "[1,\"" + randomStop.id + "\",\"" + f.testBusRoute1.id + "\",1,\"Any Place\",\"SampleReg\"," + f.generateArrivalTime  + "]"
    val invalidSourceLine = "[1,\"" + "XXSTOP" + "\",\"" + f.testBusRoute1.id + "\",1,\"Any Place\",\"SampleReg\"," + f.generateArrivalTime  + "]"
    val testLines = List(validSourceline, invalidSourceLine)

    val testConfig = f.testDataSourceConfig.copy(simulationIterator = Some(testLines.toIterator))
    val dataStreamProcessorTest = new DataStreamProcessor(testConfig, f.testMessagingConfig, f.historicalSourceLineProcessor)(f.actorSystem)

    dataStreamProcessorTest.start
    Thread.sleep(500)
    dataStreamProcessorTest.stop

    eventually {
      dataStreamProcessorTest.numberLinesProcessed shouldBe 2
      f.historicalSourceLineProcessor.getNumberProcessed shouldBe 2
      testLines should contain (sourceLineBackToLine(f.historicalSourceLineProcessor.lastProcessedSourceLine.get))
      validSourceline shouldEqual validatedSourceLineBackToLine(f.historicalSourceLineProcessor.lastValidatedSourceLine.get)
      f.historicalSourceLineProcessor.getNumberValidated shouldBe 1
    }
  }

  test("Duplicate lines received within x seconds should not be processed") { f =>

    val routeDefFromDb = f.definitions(f.testBusRoute1)
    def randomStop = routeDefFromDb(Random.nextInt(routeDefFromDb.size - 1))

    val validSourceline = "[1,\"" + randomStop.id + "\",\"" + f.testBusRoute1.id + "\",1,\"Any Place\",\"SampleReg\"," + f.generateArrivalTime + "]"
    val testLines = List(validSourceline, validSourceline, validSourceline)

    val testConfig = f.testDataSourceConfig.copy(simulationIterator = Some(testLines.toIterator))
    val dataStreamProcessorTest = new DataStreamProcessor(testConfig, f.testMessagingConfig, f.historicalSourceLineProcessor)(f.actorSystem)

    dataStreamProcessorTest.start
    Thread.sleep(500)
    dataStreamProcessorTest.stop

    eventually {
      dataStreamProcessorTest.numberLinesProcessed shouldBe 3
      f.historicalSourceLineProcessor.getNumberProcessed shouldBe 3
      testLines should contain (sourceLineBackToLine(f.historicalSourceLineProcessor.lastProcessedSourceLine.get))
      validSourceline shouldEqual validatedSourceLineBackToLine(f.historicalSourceLineProcessor.lastValidatedSourceLine.get)
      f.historicalSourceLineProcessor.getNumberValidated shouldBe 1
    }
  }

  test("Lines received with arrival time in past should be ignored"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute1)
    val arrivalTime = f.generateArrivalTime

    val testLines: List[String] = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute1.id + "\",1,\"Any Place\",\"V123456\"," + arrivalTime + "]")

    val splitLines = testLines.splitAt(testLines.size / 2)
    val pastLine = List(splitLines._2.head.replace(arrivalTime.toString, (arrivalTime - 100000).toString))
    val joinedLines = splitLines._1 ++ pastLine ++ splitLines._2.tail

    val testConfig = f.testDataSourceConfig.copy(simulationIterator = Some(testLines.toIterator))
    val dataStreamProcessorTest = new DataStreamProcessor(testConfig, f.testMessagingConfig, f.historicalSourceLineProcessor)(f.actorSystem)

    dataStreamProcessorTest.start
    Thread.sleep(500)
    dataStreamProcessorTest.stop

    eventually {
      dataStreamProcessorTest.numberLinesProcessed shouldBe joinedLines.size
      f.historicalSourceLineProcessor.getNumberProcessed shouldBe joinedLines.size
      f.historicalSourceLineProcessor.getNumberValidated shouldBe joinedLines.size - 1
      f.historicalSourceLineProcessor.getCurrentActors.futureValue.size shouldBe 1
    }
  }

  test("An actor should be created for each new vehicle received with a new route") { f=>

    val routeDef1FromDb = f.definitions(f.testBusRoute1)
    val routeDef2FromDb = f.definitions(f.testBusRoute2)
    def randomStopRoute1 = routeDef1FromDb(Random.nextInt(routeDef1FromDb.size - 1))
    def randomStopRoute2 = routeDef2FromDb(Random.nextInt(routeDef2FromDb.size - 1))

    val validSourceline1 = "[1,\"" + randomStopRoute1.id + "\",\"" + f.testBusRoute1.id + "\",1,\"Any Place\",\"V123456\"," + f.generateArrivalTime + "]"
    val validSourceline2 = "[1,\"" + randomStopRoute1.id + "\",\"" + f.testBusRoute1.id + "\",1,\"Any Place\",\"V123456\"," + f.generateArrivalTime + "]"
    val validSourceline3 = "[1,\"" + randomStopRoute1.id + "\",\"" + f.testBusRoute1.id + "\",1,\"Any Place\",\"V23456\"," + f.generateArrivalTime + "]"
    val validSourceline4 = "[1,\"" + randomStopRoute2.id + "\",\"" + f.testBusRoute2.id + "\",2,\"Any Place\",\"V23456\"," + f.generateArrivalTime + "]"

    val testLines = List(validSourceline1, validSourceline2, validSourceline3, validSourceline4)

    val testConfig = f.testDataSourceConfig.copy(simulationIterator = Some(testLines.toIterator))
    val dataStreamProcessorTest = new DataStreamProcessor(testConfig, f.testMessagingConfig, f.historicalSourceLineProcessor)(f.actorSystem)

    dataStreamProcessorTest.start
    Thread.sleep(500)
    dataStreamProcessorTest.stop

    eventually {
      dataStreamProcessorTest.numberLinesProcessed shouldBe 4
      f.historicalSourceLineProcessor.getNumberProcessed shouldBe 4
      f.historicalSourceLineProcessor.getNumberValidated shouldBe 4
      f.historicalSourceLineProcessor.getCurrentActors.futureValue.size shouldBe 3
    }
  }

  test("Vehicle actors should add incoming lines relating to arrival times to stop record"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute1)

    val testLines = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute1.id + "\",1,\"Any Place\",\"V123456\"," + f.generateArrivalTime + "]")

    val testConfig = f.testDataSourceConfig.copy(simulationIterator = Some(testLines.toIterator))
    val dataStreamProcessorTest = new DataStreamProcessor(testConfig, f.testMessagingConfig, f.historicalSourceLineProcessor)(f.actorSystem)

    dataStreamProcessorTest.start
    Thread.sleep(500)
    dataStreamProcessorTest.stop

    eventually {
      dataStreamProcessorTest.numberLinesProcessed shouldBe testLines.size
      f.historicalSourceLineProcessor.getNumberProcessed shouldBe testLines.size
      f.historicalSourceLineProcessor.getNumberValidated shouldBe testLines.size
      f.historicalSourceLineProcessor.getCurrentActors.futureValue.size shouldBe 1
      f.historicalSourceLineProcessor.getArrivalRecords("V123456", f.testBusRoute1).futureValue.size shouldBe testLines.size
    }
  }


  test("Vehicle actors should update record when closer arrival time is received"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute1)
    val arrivalTime = System.currentTimeMillis() + 100000

    val testLines = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute1.id + "\",1,\"Any Place\",\"V123456\"," + arrivalTime + "]")

    val newArrivalTime = arrivalTime - 50000
    val newRecord = testLines.head.replace(arrivalTime.toString, newArrivalTime.toString)
    val testLinesWithUpdated = testLines ++ List(newRecord)

    val testConfig = f.testDataSourceConfig.copy(simulationIterator = Some(testLinesWithUpdated.toIterator))
    val dataStreamProcessorTest = new DataStreamProcessor(testConfig, f.testMessagingConfig, f.historicalSourceLineProcessor)(f.actorSystem)

    dataStreamProcessorTest.start
    Thread.sleep(500)
    dataStreamProcessorTest.stop

    eventually {
      dataStreamProcessorTest.numberLinesProcessed shouldBe testLinesWithUpdated.size
      f.historicalSourceLineProcessor.getNumberProcessed shouldBe testLinesWithUpdated.size
      f.historicalSourceLineProcessor.getNumberValidated shouldBe testLinesWithUpdated.size
      f.historicalSourceLineProcessor.getCurrentActors.futureValue.size shouldBe 1
      f.historicalSourceLineProcessor.getArrivalRecords("V123456", f.testBusRoute1).futureValue.apply(routeDefFromDb.head) shouldBe newArrivalTime
    }
  }

  test("Vehicle actors should persist record if stop arrival time information is complete and passes validation"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute1)
    val vehicleReg = "V123456"

    val testLines: List[String] = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute1.id + "\",1,\"Any Place\",\"" + vehicleReg + "\"," + f.generateArrivalTime + "]")

    val testConfig = f.testDataSourceConfig.copy(simulationIterator = Some(testLines.toIterator))
    val dataStreamProcessorTest = new DataStreamProcessor(testConfig, f.testMessagingConfig, f.historicalSourceLineProcessor)(f.actorSystem)

    dataStreamProcessorTest.start
    Thread.sleep(500)
    dataStreamProcessorTest.stop

    eventually {
      f.historicalSourceLineProcessor.getCurrentActors.futureValue.size shouldBe 1
    }

    eventually {
      f.historicalSourceLineProcessor.vehicleActorSupervisor ! PersistAndRemoveInactiveVehicles
      dataStreamProcessorTest.numberLinesProcessed shouldBe testLines.size
      f.historicalSourceLineProcessor.getNumberProcessed shouldBe testLines.size
      f.historicalSourceLineProcessor.getNumberValidated shouldBe testLines.size
      f.historicalSourceLineProcessor.getCurrentActors.futureValue.size shouldBe 0
      f.testHistoricalRecordsCollection.numberInsertsRequested shouldBe 1
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).exists(result => result.vehicleID == vehicleReg) shouldBe true
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).exists(result => result.busRoute == f.testBusRoute1) shouldBe true
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).filter(result => result.vehicleID == vehicleReg).head.stopRecords.size shouldBe testLines.size
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).filter(result => result.vehicleID == vehicleReg).head.stopRecords.map(record => record.stopID) shouldEqual routeDefFromDb.map(stop => stop.id)

    }
  }

  test("Persisted record should be loaded with stop sequence in same order and with same values"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute1)
    val vehicleReg = "V123456"

    val testLines: List[String] = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute1.id + "\",1,\"Any Place\",\"" + vehicleReg + "\"," + f.generateArrivalTime + "]")

    val testConfig = f.testDataSourceConfig.copy(simulationIterator = Some(testLines.toIterator))
    val dataStreamProcessorTest = new DataStreamProcessor(testConfig, f.testMessagingConfig, f.historicalSourceLineProcessor)(f.actorSystem)

    dataStreamProcessorTest.start
    Thread.sleep(500)
    dataStreamProcessorTest.stop

    eventually {
      f.historicalSourceLineProcessor.getCurrentActors.futureValue.size shouldBe 1
    }

    eventually {
      f.historicalSourceLineProcessor.vehicleActorSupervisor ! PersistAndRemoveInactiveVehicles
      dataStreamProcessorTest.numberLinesProcessed shouldBe testLines.size
      f.historicalSourceLineProcessor.getNumberProcessed shouldBe testLines.size
      f.historicalSourceLineProcessor.getNumberValidated shouldBe testLines.size
      f.historicalSourceLineProcessor.getCurrentActors.futureValue.size shouldBe 0
      f.testHistoricalRecordsCollection.numberInsertsRequested shouldBe 1
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).exists(result => result.vehicleID == vehicleReg) shouldBe true
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).exists(result => result.busRoute == f.testBusRoute1) shouldBe true
      val historicalRecord = f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).filter(result => result.vehicleID == vehicleReg).head
      historicalRecord.stopRecords.map(record => record.stopID) shouldEqual routeDefFromDb.map(stop => stop.id)
    }
  }

  test("Vehicle actors should not persist record if there is a gap in the sequence"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute1)
    val vehicleReg = "V123456"

    val testLines: List[String] = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute1.id + "\",1,\"Any Place\",\"" + vehicleReg + "\"," + f.generateArrivalTime + "]")

    val splitTestLines = testLines.splitAt(testLines.size / 2)
    val testLinesWithMissing = splitTestLines._1 ++ splitTestLines._2.tail

    val testConfig = f.testDataSourceConfig.copy(simulationIterator = Some(testLinesWithMissing.toIterator))
    val dataStreamProcessorTest = new DataStreamProcessor(testConfig, f.testMessagingConfig, f.historicalSourceLineProcessor)(f.actorSystem)

    dataStreamProcessorTest.start
    Thread.sleep(500)
    dataStreamProcessorTest.stop

    eventually {
      f.historicalSourceLineProcessor.getCurrentActors.futureValue.size shouldBe 1
    }
    eventually {
      f.historicalSourceLineProcessor.vehicleActorSupervisor ! PersistAndRemoveInactiveVehicles
      dataStreamProcessorTest.numberLinesProcessed shouldBe testLinesWithMissing.size
      f.historicalSourceLineProcessor.getNumberProcessed shouldBe testLinesWithMissing.size
      f.historicalSourceLineProcessor.getNumberValidated shouldBe testLinesWithMissing.size
      f.historicalSourceLineProcessor.getCurrentActors.futureValue.size shouldBe 0
      f.testHistoricalRecordsCollection.numberInsertsRequested shouldBe 0
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).exists(result => result.vehicleID == vehicleReg) shouldBe false
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).exists(result => result.busRoute == f.testBusRoute1) shouldBe false
    }
  }

  test("Vehicle actors should not persist record if the number of stops received falls below the minimum required"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute1)
    val vehicleReg = "V123456"

    val testLines: List[String] = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute1.id + "\",1,\"Any Place\",\"" + vehicleReg + "\"," + f.generateArrivalTime + "]")

    val testLinesLast4 = testLines.takeRight(4)

    val testConfig = f.testDataSourceConfig.copy(simulationIterator = Some(testLinesLast4.toIterator))
    val dataStreamProcessorTest = new DataStreamProcessor(testConfig, f.testMessagingConfig, f.historicalSourceLineProcessor)(f.actorSystem)

    dataStreamProcessorTest.start
    Thread.sleep(500)
    dataStreamProcessorTest.stop

    eventually {
      f.historicalSourceLineProcessor.getCurrentActors.futureValue.size shouldBe 1
    }
    eventually {
      f.historicalSourceLineProcessor.vehicleActorSupervisor ! PersistAndRemoveInactiveVehicles
      dataStreamProcessorTest.numberLinesProcessed shouldBe testLinesLast4.size
      f.historicalSourceLineProcessor.getNumberProcessed shouldBe testLinesLast4.size
      f.historicalSourceLineProcessor.getNumberValidated shouldBe testLinesLast4.size
      f.historicalSourceLineProcessor.getCurrentActors.futureValue.size shouldBe 0
      f.testHistoricalRecordsCollection.numberInsertsRequested shouldBe 0
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).exists(result => result.vehicleID == vehicleReg) shouldBe false
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).exists(result => result.busRoute == f.testBusRoute1) shouldBe false
    }
  }

  test("Vehicle actors should persist record if there is a gap at the beginning of the sequence (bus started midway through route)"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute1)
    val vehicleReg = "V123456"

    val testLines: List[String] = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute1.id + "\",1,\"Any Place\",\"" + vehicleReg + "\"," + f.generateArrivalTime + "]")

    val testLinesSecondHalf = testLines.splitAt(testLines.size / 2)._2

    val testConfig = f.testDataSourceConfig.copy(simulationIterator = Some(testLinesSecondHalf.toIterator))
    val dataStreamProcessorTest = new DataStreamProcessor(testConfig, f.testMessagingConfig, f.historicalSourceLineProcessor)(f.actorSystem)

    dataStreamProcessorTest.start
    Thread.sleep(500)
    dataStreamProcessorTest.stop

    eventually {
      f.historicalSourceLineProcessor.getCurrentActors.futureValue.size shouldBe 1
    }

    eventually {
      f.historicalSourceLineProcessor.vehicleActorSupervisor ! PersistAndRemoveInactiveVehicles
      dataStreamProcessorTest.numberLinesProcessed shouldBe testLinesSecondHalf.size
      f.historicalSourceLineProcessor.getNumberProcessed shouldBe testLinesSecondHalf.size
      f.historicalSourceLineProcessor.getNumberValidated shouldBe testLinesSecondHalf.size
      f.historicalSourceLineProcessor.getCurrentActors.futureValue.size shouldBe 0
      f.testHistoricalRecordsCollection.numberInsertsRequested shouldBe 1
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).exists(result => result.vehicleID == vehicleReg) shouldBe true
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).exists(result => result.busRoute == f.testBusRoute1) shouldBe true
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).count(result => result.busRoute == f.testBusRoute1) shouldBe 1
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).filter(result => result.vehicleID == vehicleReg).head.stopRecords.size shouldBe testLinesSecondHalf.size
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).filter(result => result.vehicleID == vehicleReg).head.stopRecords.map(record => record.stopID) shouldEqual routeDefFromDb.map(stop => stop.id).splitAt(routeDefFromDb.size / 2)._2
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).filter(result => result.vehicleID == vehicleReg).head.stopRecords.map(record => routeDefFromDb.indexWhere(x => x.id == record.stopID)) shouldEqual f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).filter(result => result.vehicleID == vehicleReg).head.stopRecords.map(record => record.seqNo)
    }
  }

  test("Multiple records should be persisted where the same bus on the same route makes the same journey after period of inactivity"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute1)
    val vehicleReg = "V123456"

    val testLines1: List[String] = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute1.id + "\",1,\"Any Place\",\"" + vehicleReg + "\"," + f.generateArrivalTime + "]")

    val testConfig1 = f.testDataSourceConfig.copy(simulationIterator = Some(testLines1.toIterator))
    val dataStreamProcessorTest1 = new DataStreamProcessor(testConfig1, f.testMessagingConfig, f.historicalSourceLineProcessor)(f.actorSystem)

    dataStreamProcessorTest1.start
    Thread.sleep(500)
    dataStreamProcessorTest1.stop
    Thread.sleep(7000)

    f.historicalSourceLineProcessor.vehicleActorSupervisor ! PersistAndRemoveInactiveVehicles
    Thread.sleep(1000)
    val testLines2: List[String] = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute1.id + "\",1,\"Any Place\",\"" + vehicleReg + "\"," + f.generateArrivalTime + "]")

    val testConfig2 = f.testDataSourceConfig.copy(simulationIterator = Some(testLines2.toIterator))
    val dataStreamProcessorTest2 = new DataStreamProcessor(testConfig2, f.testMessagingConfig, f.historicalSourceLineProcessor)(f.actorSystem)

    dataStreamProcessorTest2.start
    Thread.sleep(500)
    dataStreamProcessorTest2.stop

    eventually {
      f.historicalSourceLineProcessor.getCurrentActors.futureValue.size shouldBe 1
    }
    eventually {
      f.historicalSourceLineProcessor.vehicleActorSupervisor ! PersistAndRemoveInactiveVehicles
      f.historicalSourceLineProcessor.getNumberProcessed shouldBe testLines1.size + testLines2.size
      f.historicalSourceLineProcessor.getNumberValidated shouldBe testLines1.size + testLines2.size
      f.historicalSourceLineProcessor.getCurrentActors.futureValue.size shouldBe 0
      f.testHistoricalRecordsCollection.numberInsertsRequested shouldBe 2
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).exists(result => result.vehicleID == vehicleReg) shouldBe true
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).count(result => result.busRoute == f.testBusRoute1) shouldBe 2
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).filter(result => result.vehicleID == vehicleReg).head.stopRecords.size shouldBe testLines1.size
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).filter(result => result.vehicleID == vehicleReg)(1).stopRecords.size shouldBe testLines2.size
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).filter(result => result.vehicleID == vehicleReg).head.stopRecords.map(record => record.stopID) shouldEqual routeDefFromDb.map(stop => stop.id)
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).filter(result => result.vehicleID == vehicleReg)(1).stopRecords.map(record => record.stopID) shouldEqual routeDefFromDb.map(stop => stop.id)
    }
  }

  test("Only one record should be persisted if multiple persists are requested for the same bus making the same route at the same time"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute1)
    val vehicleReg = "V123456"

    val testLines1: List[String] = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute1.id + "\",1,\"Any Place 1\",\"" + vehicleReg + "\"," + f.generateArrivalTime + "]")

    val testLines2: List[String] = testLines1.map(line => line.replace("Any Place 1", "Any Place 2"))

    val testLinesDoubled = testLines1 ++ testLines2

    val testConfig = f.testDataSourceConfig.copy(simulationIterator = Some(testLinesDoubled.toIterator))
    val dataStreamProcessorTest = new DataStreamProcessor(testConfig, f.testMessagingConfig, f.historicalSourceLineProcessor)(f.actorSystem)

    dataStreamProcessorTest.start
    Thread.sleep(500)
    dataStreamProcessorTest.stop

    eventually {
      f.historicalSourceLineProcessor.getCurrentActors.futureValue.size shouldBe 1
    }

    eventually {
      f.historicalSourceLineProcessor.vehicleActorSupervisor ! PersistAndRemoveInactiveVehicles
      dataStreamProcessorTest.numberLinesProcessed shouldBe testLinesDoubled.size
      f.historicalSourceLineProcessor.getNumberProcessed shouldBe testLinesDoubled.size
      f.historicalSourceLineProcessor.getNumberValidated shouldBe testLinesDoubled.size
      f.historicalSourceLineProcessor.getCurrentActors.futureValue.size shouldBe 0
      f.testHistoricalRecordsCollection.numberInsertsRequested shouldBe 1
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).exists(result => result.vehicleID == vehicleReg) shouldBe true
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).count(result => result.busRoute == f.testBusRoute1) shouldBe 1
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).filter(result => result.vehicleID == vehicleReg).head.stopRecords.size shouldBe testLines1.size
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute1).filter(result => result.vehicleID == vehicleReg).head.stopRecords.map(record => record.stopID) shouldEqual routeDefFromDb.map(stop => stop.id)
    }
  }

  test("Vehicles should persist and be shut down after specified period of inactivity"){f =>
    f.dataStreamProcessingControllerReal.stop
    f.actorSystem.terminate().futureValue
    f.testDefinitionsCollection.db.dropDatabase

    val tempFixture = new HistoricalTestFixture(5000)

    try {
      val routeDefFromDb = tempFixture.definitions(tempFixture.testBusRoute1)
      val vehicleReg = "V123456"

      val testLines: List[String] = routeDefFromDb.map(busStop =>
        "[1,\"" + busStop.id + "\",\"" + tempFixture.testBusRoute1.id + "\",1,\"Any Place\",\"" + vehicleReg + "\"," + tempFixture.generateArrivalTime + "]")

      val testLinesFirstHalf = testLines.splitAt(testLines.size / 2)._1

      val testConfig = f.testDataSourceConfig.copy(simulationIterator = Some(testLinesFirstHalf.toIterator))
      val dataStreamProcessorTest = new DataStreamProcessor(testConfig, f.testMessagingConfig, f.historicalSourceLineProcessor)(f.actorSystem)

      dataStreamProcessorTest.start
      Thread.sleep(500)
      dataStreamProcessorTest.stop
      Thread.sleep(7000) //Period of inactivity
      val mockValidatedSourceLine = ValidatedSourceLine(tempFixture.testBusRoute1, routeDefFromDb.head, "direction", "V98765", System.currentTimeMillis())
      tempFixture.historicalSourceLineProcessor.vehicleActorSupervisor ! mockValidatedSourceLine //A new line is needed to prompt the vehicle actor to clean up

      eventually {
        dataStreamProcessorTest.numberLinesProcessed shouldBe testLinesFirstHalf.size
        tempFixture.historicalSourceLineProcessor.getNumberProcessed shouldBe testLinesFirstHalf.size
        tempFixture.historicalSourceLineProcessor.getNumberValidated shouldBe testLinesFirstHalf.size
        tempFixture.historicalSourceLineProcessor.getCurrentActors.futureValue.size shouldBe 1
        tempFixture.testHistoricalRecordsCollection.numberInsertsRequested shouldBe 1
        tempFixture.testHistoricalRecordsCollection.getHistoricalRecordFromDB(tempFixture.testBusRoute1).exists(result => result.vehicleID == vehicleReg) shouldBe true
        tempFixture.testHistoricalRecordsCollection.getHistoricalRecordFromDB(tempFixture.testBusRoute1).exists(result => result.busRoute == tempFixture.testBusRoute1) shouldBe true
        tempFixture.testHistoricalRecordsCollection.getHistoricalRecordFromDB(tempFixture.testBusRoute1).count(result => result.busRoute == tempFixture.testBusRoute1) shouldBe 1
        tempFixture.testHistoricalRecordsCollection.getHistoricalRecordFromDB(tempFixture.testBusRoute1).filter(result => result.vehicleID == vehicleReg).head.stopRecords.size shouldBe testLinesFirstHalf.size
      }
    } finally {
      tempFixture.dataStreamProcessingControllerReal.stop
      tempFixture.actorSystem.terminate().futureValue
      tempFixture.testDefinitionsCollection.db.dropDatabase
    }
  }

  def sourceLineBackToLine(sourceLine: SourceLine): String = {
    "[1,\"" + sourceLine.stopID + "\",\"" + sourceLine.route + "\"," + sourceLine.direction + ",\"" + sourceLine.destinationText + "\",\"" + sourceLine.vehicleID + "\"," + sourceLine.arrival_TimeStamp + "]"
  }
  def validatedSourceLineBackToLine(sourceLine: ValidatedSourceLine): String = {
    def directionToInt(direction: String): Int = direction match {
      case "outbound" => 1
      case "inbound" => 2
    }
    "[1,\"" + sourceLine.busStop.id + "\",\"" + sourceLine.busRoute.id + "\"," + directionToInt(sourceLine.busRoute.direction) + ",\"" + sourceLine.destinationText + "\",\"" + sourceLine.vehicleID + "\"," + sourceLine.arrival_TimeStamp + "]"
  }
}
