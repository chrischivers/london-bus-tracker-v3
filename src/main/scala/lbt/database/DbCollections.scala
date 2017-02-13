package lbt.database

import lbt.{ConfigLoader, DatabaseConfig, DefinitionsConfig}
import lbt.database.definitions.BusDefinitionsCollection

object DbCollections {
  private val defaultDefConfig = ConfigLoader.defaultConfig.definitionsConfig
    private val defaultDbConfig = ConfigLoader.defaultConfig.databaseConfig

    def loadBusDefinitionsCollection(defConfig: DefinitionsConfig = defaultDefConfig, dbConfig: DatabaseConfig = defaultDbConfig) = {
      new BusDefinitionsCollection(defConfig, dbConfig)
    }

}
