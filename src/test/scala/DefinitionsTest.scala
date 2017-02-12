import lbt.ConfigLoader
import lbt.dataSource.definitions.BusDefinitions
import org.scalatest.FunSuite

class DefinitionsTest extends FunSuite {
  val config = ConfigLoader.defaultConfig.definitionsConfig

  BusDefinitions.refreshBusRouteDefinitionFromWeb(updateNewRoutesOnly = false, config)

}
