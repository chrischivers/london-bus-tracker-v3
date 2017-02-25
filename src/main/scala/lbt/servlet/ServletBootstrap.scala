package lbt.servlet

import javax.servlet.ServletContext

import lbt.database.definitions.BusDefinitionsCollection
import org.scalatra._

class ServletBootstrap(busDefinitionsCollection: BusDefinitionsCollection) extends LifeCycle {
  override def init(context: ServletContext) {
    context.mount(new LbtServlet(busDefinitionsCollection), "/*")
  }
}
