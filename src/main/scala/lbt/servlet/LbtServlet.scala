package lbt.servlet

import org.scalatra.ScalatraServlet

class LbtServlet extends ScalatraServlet {

  get("/") {
    <html>
      <body>
        <h1>Hello, world!</h1>
        Say <a href="hello-scalate">hello to Scalate</a>.
      </body>
    </html>
  }

}
