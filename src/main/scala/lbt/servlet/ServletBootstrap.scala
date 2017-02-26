package lbt.servlet

import javax.servlet.ServletContext

import lbt.database.definitions.BusDefinitionsCollection
import lbt.database.historical.HistoricalRecordsCollection
import org.scalatra._

class ServletBootstrap(busDefinitionsCollection: BusDefinitionsCollection, historicalRecordsCollection: HistoricalRecordsCollection) extends LifeCycle {
  override def init(context: ServletContext) {
    context.mount(new LbtServlet(busDefinitionsCollection, historicalRecordsCollection), "/*")
  }
}
