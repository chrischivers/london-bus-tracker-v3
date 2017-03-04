import java.util
import java.util.EventListener

import org.scalatra.LifeCycle
import javax.servlet.{Filter, Servlet, ServletContext, SessionTrackingMode}

import scala.concurrent.ExecutionContext.Implicits.global
import lbt.{ConfigLoader, Main}
import lbt.comon.BusRoute
import lbt.database.definitions.BusDefinitionsCollection
import lbt.database.historical.HistoricalRecordsCollection
import lbt.servlet.LbtServlet

class ScalatraBootstrap extends LifeCycle {

  override def init(context: ServletContext) {
    context mount (new LbtServlet(Main.definitionsCollection, Main.historicalRecordsCollection, Main.dataStreamProcessor), "/*")
  }
}