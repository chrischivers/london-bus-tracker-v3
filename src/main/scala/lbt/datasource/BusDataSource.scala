package lbt.datasource

import java.io.{BufferedReader, InputStreamReader}
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.scalalogging.StrictLogging
import lbt.datasource.streaming.SourceLineValidator
import lbt.{ConfigLoader, DataSourceConfig}
import org.apache.http.HttpStatus
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.CredentialsProvider
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{BasicCredentialsProvider, CloseableHttpClient, HttpClientBuilder}


case class SourceLine(route: String, direction: Int, stopID: String, destinationText: String, vehicleID: String, arrival_TimeStamp: Long)

object BusDataSource {

  private val defaultDataSourceConfig = ConfigLoader.defaultConfig.dataSourceConfig
  private var activeDataSource: Option[BusDataSource] = None

  def apply(): BusDataSource = apply(defaultDataSourceConfig)

  def apply(config: DataSourceConfig): BusDataSource = {
    activeDataSource match {
      case Some(source) =>
        source.closeClient()
      case None => //Do Nothing
    }
    val newDataSource = new BusDataSource(config)
    activeDataSource = Some(newDataSource)
    newDataSource
  }

  class BusDataSource(config: DataSourceConfig) extends Iterator[SourceLine] with StrictLogging {

    private val dataSourceClient = new BusDataSourceHttpClient(config)
    private val dataSourceIterator = dataSourceClient.dataStream
    protected val numberLinesStreamed = new AtomicLong(0)

    override def hasNext: Boolean = dataSourceIterator.hasNext

    override def next() = {
      numberLinesStreamed.incrementAndGet()
      SourceLineValidator(dataSourceIterator.next())
    }

    def closeClient() = {
      logger.info("closing http client")
      dataSourceClient.closeDataSource
    }

    def getNumberLinesStreamed = numberLinesStreamed.get()
  }

  private class BusDataSourceHttpClient(config: DataSourceConfig) extends StrictLogging {

    private val httpRequestConfig = buildHttpRequestConfig(config.timeout)
    private val httpAuthScope = buildAuthScope(config.authScopeURL, config.authScopePort)
    private val httpCredentialsProvider = buildHttpCredentialsProvider(config.username, config.password, httpAuthScope)
    private val httpClient: CloseableHttpClient = buildHttpClient(httpRequestConfig, httpCredentialsProvider)
    private val httpResponse: CloseableHttpResponse = getHttpResponse(httpClient, config.sourceUrl)
    val dataStream = getStream(httpResponse).iterator

    private def getStream(httpResponse: CloseableHttpResponse) = {
      httpResponse.getStatusLine.getStatusCode match {
        case HttpStatus.SC_OK =>
          val br = new BufferedReader(new InputStreamReader(httpResponse.getEntity.getContent))
          Stream.continually(br.readLine()).takeWhile(_ != null).drop(config.linesToDisregard)
        case otherStatus: Int =>
          logger.debug(s"Error getting Stream Iterator. Http Status Code: $otherStatus")
          throw new IllegalStateException("Unable to retrieve input stream")
      }
    }

    private def getHttpResponse(client: CloseableHttpClient, url: String): CloseableHttpResponse = {
      val httpGet = new HttpGet(url)
      client.execute(httpGet)
    }

    private def buildHttpClient(requestConfig: RequestConfig, credentialsProvider: CredentialsProvider): CloseableHttpClient = {
      val client = HttpClientBuilder.create()
      client.setDefaultRequestConfig(requestConfig)
      client.setDefaultCredentialsProvider(credentialsProvider)
      client.build()
    }

    private def buildHttpRequestConfig(connectionTimeout: Int): RequestConfig = {
      val requestBuilder = RequestConfig.custom()
      requestBuilder.setConnectionRequestTimeout(connectionTimeout)
      requestBuilder.setConnectTimeout(connectionTimeout)
      requestBuilder.build()
    }

    private def buildHttpCredentialsProvider(userName: String, password: String, authScope: AuthScope): BasicCredentialsProvider = {
      val credentialsProvider = new BasicCredentialsProvider()
      val credentials = new UsernamePasswordCredentials(userName, password)
      credentialsProvider.setCredentials(authScope, credentials)
      credentialsProvider
    }

    private def buildAuthScope(authScopeUrl: String, authScopePort: Int): AuthScope = {
      new AuthScope(authScopeUrl, authScopePort)
    }

    def closeDataSource = {
      logger.info("closing http client")
      httpClient.close()
      httpResponse.close()
    }
  }

}
