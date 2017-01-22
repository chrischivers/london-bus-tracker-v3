package lbt

import com.typesafe.config.ConfigFactory

case class DataSourceConfig(sourceUrl: String, username: String, password: String, authScopeURL: String, authScopePort: Int, timeout: Int, linesToDisregard: Int, waitTimeAfterClose: Int)

case class LBTConfig(
                   dataSourceConfig: DataSourceConfig
                      )

object ConfigLoader {

  private val defaultConfigFactory = ConfigFactory.load()

  val defaultConfig: LBTConfig = {
    val dataSourceStreamingParamsPrefix = "dataSource.streaming-parameters."
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
        )
    )

  }



}
