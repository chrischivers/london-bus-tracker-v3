package lbt

import com.typesafe.config.ConfigFactory
import lbt.comon.BusRoute
import net.liftweb.json._

import scala.collection.JavaConversions._

case class DataSourceConfig(sourceUrl: String, username: String, password: String, authScopeURL: String, authScopePort: Int, timeout: Int, linesToDisregard: Int, waitTimeAfterClose: Int, cacheTimeToLiveSeconds: Int, timeWindowToAcceptLines: Int, numberEmptyIteratorCasesBeforeRestart: Int, getOnlyRoutes: Option[List[BusRoute]], simulationIterator: Option[Iterator[String]] = None)

case class DefinitionsConfig(sourceAllUrl: String, sourceSingleUrl: String, definitionsCachedTime: Int)

case class DatabaseConfig(busDefinitionsTableName: String, historicalRecordsTableName: String)

case class HistoricalRecordsConfig(vehicleInactivityTimeBeforePersist: Long, numberOfLinesToCleanupAfter: Int, minimumNumberOfStopsToPersist: Int, toleranceForFuturePredictions: Long, defaultRetrievalLimit: Int)

case class LBTConfig(
                   dataSourceConfig: DataSourceConfig,
                   databaseConfig: DatabaseConfig,
                   definitionsConfig: DefinitionsConfig,
                   historicalRecordsConfig: HistoricalRecordsConfig)

object ConfigLoader {

  implicit val formats = DefaultFormats
  private val defaultConfigFactory = ConfigFactory.load()

  val defaultConfig: LBTConfig = {
    val dataSourceStreamingParamsPrefix = "dataSource.streaming-parameters."
    val dataBaseParamsPrefix = "database."
    val definitionsParamsPrefix = "dataSource.definitions."
    val historicalRecordsParamsPrefix = "lbt.historical-records."
    LBTConfig(
      DataSourceConfig(
        defaultConfigFactory.getString(dataSourceStreamingParamsPrefix + "tfl-url"),
        defaultConfigFactory.getString(dataSourceStreamingParamsPrefix + "username"),
        defaultConfigFactory.getString(dataSourceStreamingParamsPrefix + "password"),
        defaultConfigFactory.getString(dataSourceStreamingParamsPrefix + "authscope-url"),
        defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "authscope-port"),
        defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "connection-timeout"),
        defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "number-lines-disregarded"),
        defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "wait-time-after-close"),
        defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "cache-time-to-live-seconds"),
        defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "time-window-to-accept-lines"),
        defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "number-empty-iterator-cases-before-restart"),
        defaultConfigFactory.getStringList(dataSourceStreamingParamsPrefix + "get-only-routes").toList.map(rec => parse(rec).extract[BusRoute]) match {
          case Nil => None
          case x => Some(x)
        }
      ),
      DatabaseConfig(
        defaultConfigFactory.getString(dataBaseParamsPrefix + "bus-definitions-table-name"),
        defaultConfigFactory.getString(dataBaseParamsPrefix + "lbt.historical-table-name")
      ),
      DefinitionsConfig(
        defaultConfigFactory.getString(definitionsParamsPrefix + "definitions-all-url"),
        defaultConfigFactory.getString(definitionsParamsPrefix + "definitions-single-url"),
        defaultConfigFactory.getInt(definitionsParamsPrefix + "definitions-cached-time")
      ),
      HistoricalRecordsConfig(
        defaultConfigFactory.getLong(historicalRecordsParamsPrefix + "vehicle-inactivity-time-before-persist"),
        defaultConfigFactory.getInt(historicalRecordsParamsPrefix + "lines-to-cleanup-after"),
        defaultConfigFactory.getInt(historicalRecordsParamsPrefix + "minimum-number-of-stops-for-persist"),
        defaultConfigFactory.getLong(historicalRecordsParamsPrefix + "tolerance-for-future-predictions"),
        defaultConfigFactory.getInt(historicalRecordsParamsPrefix + "default-retrieval-limit")
      )
    )

  }
}

