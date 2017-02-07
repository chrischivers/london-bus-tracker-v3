package lbt

import com.typesafe.config.ConfigFactory

case class DataSourceConfig(sourceUrl: String, username: String, password: String, authScopeURL: String, authScopePort: Int, timeout: Int, linesToDisregard: Int, waitTimeAfterClose: Int)

case class MessagingConfig(exchangeName: String, historicalRecorderQueueName : String, historicalRecorderRoutingKey: String, liveTrackerQueueName: String, liveTrackerRoutingKey: String)

case class LBTConfig(
                   dataSourceConfig: DataSourceConfig,
                    messagingConfig: MessagingConfig
                      )

object ConfigLoader {

  private val defaultConfigFactory = ConfigFactory.load()

  val defaultConfig: LBTConfig = {
    val dataSourceStreamingParamsPrefix = "dataSource.streaming-parameters."
    val messagingParamsPrefix = "messaging.rabbitmq."
    new LBTConfig(
        new DataSourceConfig(
            defaultConfigFactory.getString(dataSourceStreamingParamsPrefix + "tfl-url"),
            defaultConfigFactory.getString(dataSourceStreamingParamsPrefix + "username"),
            defaultConfigFactory.getString(dataSourceStreamingParamsPrefix + "password"),
            defaultConfigFactory.getString(dataSourceStreamingParamsPrefix + "authscope-url"),
            defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "authscope-port"),
            defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "connection-timeout"),
            defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "number-lines-disregarded"),
            defaultConfigFactory.getInt(dataSourceStreamingParamsPrefix + "wait-time-after-close")
        ),
    new MessagingConfig(
        defaultConfigFactory.getString(messagingParamsPrefix + "exchange-name"),
        defaultConfigFactory.getString(messagingParamsPrefix + "historical-recorder-queue-name"),
        defaultConfigFactory.getString(messagingParamsPrefix + "historical-recorder-routing-key"),
        defaultConfigFactory.getString(messagingParamsPrefix + "live-tracker-queue-name"),
        defaultConfigFactory.getString(messagingParamsPrefix + "live-tracker-routing-key")
    )
    )

  }
}

