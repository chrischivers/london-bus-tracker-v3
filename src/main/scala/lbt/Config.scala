package lbt

import com.typesafe.config.ConfigFactory

case class DataSourceConfig(sourceUrl: String, username: String, password: String, authScopeURL: String, authScopePort: Int, timeout: Int, linesToDisregard: Int, waitTimeAfterClose: Int, cacheTimeToLiveSeconds: Int, timeWindowToAcceptLines: Int)

case class DefinitionsConfig(sourceAllUrl: String, sourceSingleUrl: String, definitionsCachedTime: Int)

case class DatabaseConfig(databaseName: String, busDefinitionsCollectionName: String, historicalRecordsCollectionName: String)

case class MessagingConfig(exchangeName: String, historicalRecorderQueueName : String, historicalRecorderRoutingKey: String, liveTrackerQueueName: String, liveTrackerRoutingKey: String)

case class HistoricalRecordsConfig(vehicleInactivityTimeout: Long, numberOfLinesToCleanupAfter: Int)

case class LBTConfig(
                   dataSourceConfig: DataSourceConfig,
                   databaseConfig: DatabaseConfig,
                   definitionsConfig: DefinitionsConfig,
                    messagingConfig: MessagingConfig,
                   historicalRecordsConfig: HistoricalRecordsConfig
                      )

object ConfigLoader {

  private val defaultConfigFactory = ConfigFactory.load()

  val defaultConfig: LBTConfig = {
    val dataSourceStreamingParamsPrefix = "dataSource.streaming-parameters."
    val dataBaseParamsPrefix = "database."
    val definitionsParamsPrefix = "dataSource.definitions."
    val historicalRecordsParamsPrefix = "historical-records."
    val messagingParamsPrefix = "messaging.rabbitmq."
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
        defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "time-window-to-accept-lines")
      ),
      DatabaseConfig(
        defaultConfigFactory.getString(dataBaseParamsPrefix + "database-name"),
        defaultConfigFactory.getString(dataBaseParamsPrefix + "bus-definitions-collection-name"),
        defaultConfigFactory.getString(dataBaseParamsPrefix + "historical-records-collection-name")
      ),
      DefinitionsConfig(
        defaultConfigFactory.getString(definitionsParamsPrefix + "definitions-all-url"),
        defaultConfigFactory.getString(definitionsParamsPrefix + "definitions-single-url"),
        defaultConfigFactory.getInt(definitionsParamsPrefix + "definitions-cached-time")
      ),
      MessagingConfig(
        defaultConfigFactory.getString(messagingParamsPrefix + "exchange-name"),
        defaultConfigFactory.getString(messagingParamsPrefix + "historical-recorder-queue-name"),
        defaultConfigFactory.getString(messagingParamsPrefix + "historical-recorder-routing-key"),
        defaultConfigFactory.getString(messagingParamsPrefix + "live-tracker-queue-name"),
        defaultConfigFactory.getString(messagingParamsPrefix + "live-tracker-routing-key")
      ),
      HistoricalRecordsConfig(
        defaultConfigFactory.getLong(historicalRecordsParamsPrefix + "vehicle-inactivity-timeout"),
        defaultConfigFactory.getInt(historicalRecordsParamsPrefix + "lines-to-cleanup-after")
      )
    )

  }
}

