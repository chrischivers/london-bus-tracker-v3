package lbt

import javax.servlet.ServletContext

import lbt.servlet.LbtServlet
import org.scalatra._

class ServletBootstrap extends LifeCycle {
  override def init(context: ServletContext) {
    context.mount(new LbtServlet, "/*")
  }
}
