import java.util
import java.util.EventListener

import org.scalatra.LifeCycle
import javax.servlet.{Filter, Servlet, ServletContext, SessionTrackingMode}

import scala.concurrent.ExecutionContext.Implicits.global
import lbt.{ConfigLoader, Main}
import lbt.comon.BusRoute
import lbt.database.definitions.BusDefinitionsTable
import lbt.database.historical.HistoricalTable
import lbt.servlet.LbtServlet

class ScalatraBootstrap extends LifeCycle {

  override def init(context: ServletContext) {
    context mount (new LbtServlet(Main.definitionsTable, Main.historicalTable, Main.dataStreamProcessor, Main.historicalSourceLineProcessor, Main.vehicleActorSupervisor, Main.historicalRecordsProcessor), "/*")
    context.initParameters("org.scalatra.environment") = "production"
  }
}