package historical

import datasource.TestDataSource
import lbt.comon._
import lbt.datasource.SourceLine
import lbt.datasource.streaming.DataStreamProcessingController
import lbt.historical.ValidatedSourceLine
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
      fixture.dataStreamProcessingControllerReal ! Stop
      fixture.actorSystem.terminate().futureValue
      fixture.consumer.unbindAndDelete
      fixture.testDefinitionsCollection.db.dropDatabase
    }
  }

  test("Line should be accepted if route is in definitions or disregarded if not in definitions") { f =>

    val routeDefFromDb = f.definitions(f.testBusRoute)
    def randomStop = routeDefFromDb(Random.nextInt(routeDefFromDb.size - 1))

    val validSourceline = "[1,\"" + randomStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"SampleReg\"," + f.generateArrivalTime + "]"
    val invalidSourceLine = "[1,\"" + randomStop.id + "\",\"99XXXX\",2,\"Bromley North\",\"YX62DYN\"," + f.generateArrivalTime + "]"
    val testLines = List(validSourceline, invalidSourceLine)

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLines))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe 2
      f.consumer.getNumberReceived.futureValue shouldBe 2
      f.messageProcessor.getNumberProcessed shouldBe 2
      f.messageProcessor.getNumberValidated shouldBe 1
      testLines should contain(sourceLineBackToLine(f.messageProcessor.lastProcessedMessage.get))
      validSourceline shouldEqual validatedSourceLineBackToLine(f.messageProcessor.lastValidatedMessage.get)
    }
  }

  test("Line should be accepted if bus stop is in definitions and rejected if not") { f =>

    val routeDefFromDb = f.definitions(f.testBusRoute)
    def randomStop = routeDefFromDb(Random.nextInt(routeDefFromDb.size - 1))


    val validSourceline = "[1,\"" + randomStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"SampleReg\"," + f.generateArrivalTime  + "]"
    val invalidSourceLine = "[1,\"" + "XXSTOP" + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"SampleReg\"," + f.generateArrivalTime  + "]"
    val testLines = List(validSourceline, invalidSourceLine)

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLines))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe 2
      f.consumer.getNumberReceived.futureValue shouldBe 2
      f.messageProcessor.getNumberProcessed shouldBe 2
      testLines should contain (sourceLineBackToLine(f.messageProcessor.lastProcessedMessage.get))
      validSourceline shouldEqual validatedSourceLineBackToLine(f.messageProcessor.lastValidatedMessage.get)
      f.messageProcessor.getNumberValidated shouldBe 1
    }
  }

  test("Duplicate lines received within x seconds should not be processed") { f =>

    val routeDefFromDb = f.definitions(f.testBusRoute)
    def randomStop = routeDefFromDb(Random.nextInt(routeDefFromDb.size - 1))

    val validSourceline = "[1,\"" + randomStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"SampleReg\"," + f.generateArrivalTime + "]"
    val testLines = List(validSourceline, validSourceline, validSourceline)

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLines))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe 3
      f.consumer.getNumberReceived.futureValue shouldBe 3
      f.messageProcessor.getNumberProcessed shouldBe 3
      testLines should contain (sourceLineBackToLine(f.messageProcessor.lastProcessedMessage.get))
      validSourceline shouldEqual validatedSourceLineBackToLine(f.messageProcessor.lastValidatedMessage.get)
      f.messageProcessor.getNumberValidated shouldBe 1
    }
  }

  test("Lines received with arrival time in past should be ignored"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute)
    val arrivalTime = f.generateArrivalTime

    val testLines: List[String] = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"V123456\"," + arrivalTime + "]")

    val splitLines = testLines.splitAt(testLines.size / 2)
    val pastLine = List(splitLines._2.head.replace(arrivalTime.toString, (arrivalTime - 100000).toString))
    val joinedLines = splitLines._1 ++ pastLine ++ splitLines._2.tail

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(joinedLines))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe joinedLines.size
      f.consumer.getNumberReceived.futureValue shouldBe joinedLines.size
      f.messageProcessor.getNumberProcessed shouldBe joinedLines.size
      f.messageProcessor.getNumberValidated shouldBe joinedLines.size - 1
      f.messageProcessor.getCurrentActors.futureValue.size shouldBe 1
    }
  }

  test("An actor should be created for each new vehicle received") { f=>

    val routeDefFromDb = f.definitions(f.testBusRoute)
    def randomStop = routeDefFromDb(Random.nextInt(routeDefFromDb.size - 1))

    val validSourceline1 = "[1,\"" + randomStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"V123456\"," + f.generateArrivalTime + "]"
    val validSourceline2 = "[1,\"" + randomStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"V234567\"," + f.generateArrivalTime + "]"
    val validSourceline3 = "[1,\"" + randomStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"V345678\"," + f.generateArrivalTime + "]"
    val validSourceline4 = "[1,\"" + randomStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"V345678\"," + f.generateArrivalTime + "]"

    val testLines = List(validSourceline1, validSourceline2, validSourceline3, validSourceline4)

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLines))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe 4
      f.consumer.getNumberReceived.futureValue shouldBe 4
      f.messageProcessor.getNumberProcessed shouldBe 4
      f.messageProcessor.getNumberValidated shouldBe 4
      f.messageProcessor.getCurrentActors.futureValue.size shouldBe 3
    }
  }

  test("Vehicle actors should add incoming lines relating to arrival times to stop record"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute)

    val testLines = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"V123456\"," + f.generateArrivalTime + "]")

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLines))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe testLines.size
      f.consumer.getNumberReceived.futureValue shouldBe testLines.size
      f.messageProcessor.getNumberProcessed shouldBe testLines.size
      f.messageProcessor.getNumberValidated shouldBe testLines.size
      f.messageProcessor.getCurrentActors.futureValue.size shouldBe 1
      f.messageProcessor.getArrivalRecords("V123456").futureValue.size shouldBe testLines.size
    }
  }


  test("Vehicle actors should update record when closer arrival time is received"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute)
    val arrivalTime = System.currentTimeMillis() + 100000

    val testLines = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"V123456\"," + arrivalTime + "]")

    val newArrivalTime = arrivalTime - 50000
    val newRecord = testLines.head.replace(arrivalTime.toString, newArrivalTime.toString)
    val testLinesWithUpdated = testLines ++ List(newRecord)

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLinesWithUpdated))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe testLinesWithUpdated.size
      f.consumer.getNumberReceived.futureValue shouldBe testLinesWithUpdated.size
      f.messageProcessor.getNumberProcessed shouldBe testLinesWithUpdated.size
      f.messageProcessor.getNumberValidated shouldBe testLinesWithUpdated.size
      f.messageProcessor.getCurrentActors.futureValue.size shouldBe 1
      f.messageProcessor.getArrivalRecords("V123456").futureValue.apply(routeDefFromDb.head) shouldBe newArrivalTime
    }
  }

  test("Vehicle actors should persist record if stop arrival time information is complete and passes validation"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute)
    val vehicleReg = "V123456"

    val testLines: List[String] = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"" + vehicleReg + "\"," + f.generateArrivalTime + "]")

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLines))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe testLines.size
      f.consumer.getNumberReceived.futureValue shouldBe testLines.size
      f.messageProcessor.getNumberProcessed shouldBe testLines.size
      f.messageProcessor.getNumberValidated shouldBe testLines.size
      f.messageProcessor.getCurrentActors.futureValue.size shouldBe 1
      f.testHistoricalRecordsCollection.numberInsertsRequested shouldBe 1
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).exists(result => result.vehicleID == vehicleReg) shouldBe true
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).exists(result => result.busRoute == f.testBusRoute) shouldBe true
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).filter(result => result.vehicleID == vehicleReg).head.stopRecords.size shouldBe testLines.size
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).filter(result => result.vehicleID == vehicleReg).head.stopRecords.map(record => record.stopID) shouldEqual routeDefFromDb.map(stop => stop.id)

    }
  }

  test("Persisted record should be loaded with stop sequence in same order and with same values"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute)
    val vehicleReg = "V123456"


    val testLines: List[String] = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"" + vehicleReg + "\"," + f.generateArrivalTime + "]")

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLines))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe testLines.size
      f.consumer.getNumberReceived.futureValue shouldBe testLines.size
      f.messageProcessor.getNumberProcessed shouldBe testLines.size
      f.messageProcessor.getNumberValidated shouldBe testLines.size
      f.messageProcessor.getCurrentActors.futureValue.size shouldBe 1
      f.testHistoricalRecordsCollection.numberInsertsRequested shouldBe 1
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).exists(result => result.vehicleID == vehicleReg) shouldBe true
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).exists(result => result.busRoute == f.testBusRoute) shouldBe true
      val historicalRecord = f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).filter(result => result.vehicleID == vehicleReg).head
      historicalRecord.stopRecords.map(record => record.stopID) shouldEqual routeDefFromDb.map(stop => stop.id)
    }
  }

  test("Vehicle actors should not persist record if there is a gap in the sequence"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute)
    val vehicleReg = "V123456"

    val testLines: List[String] = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"" + vehicleReg + "\"," + f.generateArrivalTime + "]")

    val splitTestLines = testLines.splitAt(testLines.size / 2)
    val testLinesWithMissing = splitTestLines._1 ++ splitTestLines._2.tail

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLinesWithMissing))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe testLinesWithMissing.size
      f.consumer.getNumberReceived.futureValue shouldBe testLinesWithMissing.size
      f.messageProcessor.getNumberProcessed shouldBe testLinesWithMissing.size
      f.messageProcessor.getNumberValidated shouldBe testLinesWithMissing.size
      f.messageProcessor.getCurrentActors.futureValue.size shouldBe 1
      f.testHistoricalRecordsCollection.numberInsertsRequested shouldBe 0
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).exists(result => result.vehicleID == vehicleReg) shouldBe false
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).exists(result => result.busRoute == f.testBusRoute) shouldBe false
    }
  }

  test("Vehicle actors should persist record if there is a gap at the beginning of the sequence (bus started midway through route)"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute)
    val vehicleReg = "V123456"

    val testLines: List[String] = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"" + vehicleReg + "\"," + f.generateArrivalTime + "]")

    val testLinesSecondHalf = testLines.splitAt(testLines.size / 2)._2

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLinesSecondHalf))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe testLinesSecondHalf.size
      f.consumer.getNumberReceived.futureValue shouldBe testLinesSecondHalf.size
      f.messageProcessor.getNumberProcessed shouldBe testLinesSecondHalf.size
      f.messageProcessor.getNumberValidated shouldBe testLinesSecondHalf.size
      f.messageProcessor.getCurrentActors.futureValue.size shouldBe 1
      f.testHistoricalRecordsCollection.numberInsertsRequested shouldBe 1
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).exists(result => result.vehicleID == vehicleReg) shouldBe true
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).exists(result => result.busRoute == f.testBusRoute) shouldBe true
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).count(result => result.busRoute == f.testBusRoute) shouldBe 1
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).filter(result => result.vehicleID == vehicleReg).head.stopRecords.size shouldBe testLinesSecondHalf.size
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).filter(result => result.vehicleID == vehicleReg).head.stopRecords.map(record => record.stopID) shouldEqual routeDefFromDb.map(stop => stop.id).splitAt(routeDefFromDb.size / 2)._2
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).filter(result => result.vehicleID == vehicleReg).head.stopRecords.map(record => routeDefFromDb.indexWhere(x => x.id == record.stopID)) shouldEqual f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).filter(result => result.vehicleID == vehicleReg).head.stopRecords.map(record => record.seqNo)
    }
  }

  test("Multiple records should be persisted where the same bus on the same route makes the same journey at a different time"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute)
    val vehicleReg = "V123456"

    val testLines1: List[String] = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"" + vehicleReg + "\"," + f.generateArrivalTime + "]")

    val testLines2: List[String] = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"" + vehicleReg + "\"," + f.generateArrivalTime + "]")

    val testLines1and2 = testLines1 ++ testLines2

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLines1and2))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe testLines1and2.size
      f.consumer.getNumberReceived.futureValue shouldBe testLines1and2.size
      f.messageProcessor.getNumberProcessed shouldBe testLines1and2.size
      f.messageProcessor.getNumberValidated shouldBe testLines1and2.size
      f.messageProcessor.getCurrentActors.futureValue.size shouldBe 1
      f.testHistoricalRecordsCollection.numberInsertsRequested shouldBe 2
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).exists(result => result.vehicleID == vehicleReg) shouldBe true
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).count(result => result.busRoute == f.testBusRoute) shouldBe 2
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).filter(result => result.vehicleID == vehicleReg).head.stopRecords.size shouldBe testLines1.size
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).filter(result => result.vehicleID == vehicleReg)(1).stopRecords.size shouldBe testLines2.size
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).filter(result => result.vehicleID == vehicleReg).head.stopRecords.map(record => record.stopID) shouldEqual routeDefFromDb.map(stop => stop.id)
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).filter(result => result.vehicleID == vehicleReg)(1).stopRecords.map(record => record.stopID) shouldEqual routeDefFromDb.map(stop => stop.id)
    }
  }

  test("Only one record should be persisted if multiple persists are requested for the same bus making the same route at the same time"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute)
    val vehicleReg = "V123456"

    val testLines1: List[String] = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place 1\",\"" + vehicleReg + "\"," + f.generateArrivalTime + "]")

    val testLines2: List[String] = testLines1.map(line => line.replace("Any Place 1", "Any Place 2"))

    val testLinesDoubled = testLines1 ++ testLines2

    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLinesDoubled))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe testLinesDoubled.size
      f.consumer.getNumberReceived.futureValue shouldBe testLinesDoubled.size
      f.messageProcessor.getNumberProcessed shouldBe testLinesDoubled.size
      f.messageProcessor.getNumberValidated shouldBe testLinesDoubled.size
      f.messageProcessor.getCurrentActors.futureValue.size shouldBe 1
      f.testHistoricalRecordsCollection.numberInsertsRequested shouldBe 2
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).exists(result => result.vehicleID == vehicleReg) shouldBe true
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).count(result => result.busRoute == f.testBusRoute) shouldBe 1
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).filter(result => result.vehicleID == vehicleReg).head.stopRecords.size shouldBe testLines1.size
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).filter(result => result.vehicleID == vehicleReg).head.stopRecords.map(record => record.stopID) shouldEqual routeDefFromDb.map(stop => stop.id)
    }
  }

  test("Record should not be duplicated in database if an updated last line is received after initial persist"){f=>
    val routeDefFromDb = f.definitions(f.testBusRoute)
    val vehicleReg = "V123456"

    val testLines: List[String] = routeDefFromDb.map(busStop =>
      "[1,\"" + busStop.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"" + vehicleReg + "\"," + f.generateArrivalTime + "]")

    val lastLineArrivalTime = f.generateArrivalTime
    val testLinesWithExtraLastLine = testLines ++ List("[1,\"" + routeDefFromDb.last.id + "\",\"" + f.testBusRoute.id + "\",1,\"Any Place\",\"" + vehicleReg + "\"," + lastLineArrivalTime + "]")
    val testDataSource = new TestDataSource(f.testDataSourceConfig, Some(testLinesWithExtraLastLine))
    val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, f.testMessagingConfig)(f.actorSystem)

    dataStreamProcessingControllerTest ! Start
    Thread.sleep(500)
    dataStreamProcessingControllerTest ! Stop

    eventually {
      testDataSource.getNumberLinesStreamed shouldBe testLinesWithExtraLastLine.size
      f.consumer.getNumberReceived.futureValue shouldBe testLinesWithExtraLastLine.size
      f.messageProcessor.getNumberProcessed shouldBe testLinesWithExtraLastLine.size
      f.messageProcessor.getNumberValidated shouldBe testLinesWithExtraLastLine.size
      f.messageProcessor.getCurrentActors.futureValue.size shouldBe 1
      f.testHistoricalRecordsCollection.numberInsertsRequested shouldBe 2
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).exists(result => result.vehicleID == vehicleReg) shouldBe true
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).exists(result => result.busRoute == f.testBusRoute) shouldBe true
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).count(result => result.busRoute == f.testBusRoute) shouldBe 1
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).filter(result => result.vehicleID == vehicleReg).head.stopRecords.size shouldBe testLines.size
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).filter(result => result.vehicleID == vehicleReg).head.stopRecords.last.arrivalTime shouldBe lastLineArrivalTime
      f.testHistoricalRecordsCollection.getHistoricalRecordFromDB(f.testBusRoute).filter(result => result.vehicleID == vehicleReg).head.stopRecords.map(record => record.stopID) shouldEqual routeDefFromDb.map(stop => stop.id)
    }
  }

  test("Vehicles should persist and be shut down after specified period of inactivity"){f =>
    f.dataStreamProcessingControllerReal ! Stop
    f.actorSystem.terminate().futureValue
    f.consumer.unbindAndDelete
    f.testDefinitionsCollection.db.dropDatabase

    val tempFixture = new HistoricalTestFixture(5000)

    try {
      val routeDefFromDb = tempFixture.definitions(tempFixture.testBusRoute)
      val vehicleReg = "V123456"

      val testLines: List[String] = routeDefFromDb.map(busStop =>
        "[1,\"" + busStop.id + "\",\"" + tempFixture.testBusRoute.id + "\",1,\"Any Place\",\"" + vehicleReg + "\"," + tempFixture.generateArrivalTime + "]")

      val testLinesFirstHalf = testLines.splitAt(testLines.size / 2)._1

      val testDataSource = new TestDataSource(tempFixture.testDataSourceConfig, Some(testLinesFirstHalf))
      val dataStreamProcessingControllerTest = DataStreamProcessingController(testDataSource, tempFixture.testMessagingConfig)(tempFixture.actorSystem)

      dataStreamProcessingControllerTest ! Start
      Thread.sleep(500)
      dataStreamProcessingControllerTest ! Stop
      Thread.sleep(7000) //Period of inactivity
      val mockValidatedSourceLine = ValidatedSourceLine(tempFixture.testBusRoute, routeDefFromDb.head, "direction", "V98765", System.currentTimeMillis())
      tempFixture.messageProcessor.vehicleActorSupervisor ! mockValidatedSourceLine //A new line is needed to prompt the vehicle actor to clean up

      eventually {
        testDataSource.getNumberLinesStreamed shouldBe testLinesFirstHalf.size
        tempFixture.consumer.getNumberReceived.futureValue shouldBe testLinesFirstHalf.size
        tempFixture.messageProcessor.getNumberProcessed shouldBe testLinesFirstHalf.size
        tempFixture.messageProcessor.getNumberValidated shouldBe testLinesFirstHalf.size
        tempFixture.messageProcessor.getCurrentActors.futureValue.size shouldBe 1
        tempFixture.testHistoricalRecordsCollection.numberInsertsRequested shouldBe 1
        tempFixture.testHistoricalRecordsCollection.getHistoricalRecordFromDB(tempFixture.testBusRoute).exists(result => result.vehicleID == vehicleReg) shouldBe true
        tempFixture.testHistoricalRecordsCollection.getHistoricalRecordFromDB(tempFixture.testBusRoute).exists(result => result.busRoute == tempFixture.testBusRoute) shouldBe true
        tempFixture.testHistoricalRecordsCollection.getHistoricalRecordFromDB(tempFixture.testBusRoute).count(result => result.busRoute == tempFixture.testBusRoute) shouldBe 1
        tempFixture.testHistoricalRecordsCollection.getHistoricalRecordFromDB(tempFixture.testBusRoute).filter(result => result.vehicleID == vehicleReg).head.stopRecords.size shouldBe testLinesFirstHalf.size
      }
    } finally {
      tempFixture.dataStreamProcessingControllerReal ! Stop
      tempFixture.actorSystem.terminate().futureValue
      tempFixture.consumer.unbindAndDelete
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
