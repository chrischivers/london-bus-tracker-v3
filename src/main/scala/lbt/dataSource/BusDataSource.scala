package lbt.dataSource

import java.io.{BufferedReader, InputStreamReader}

import com.typesafe.scalalogging.StrictLogging
import lbt.{ConfigLoader, DataSourceConfig, LBTConfig}
import org.apache.http.HttpStatus
import org.apache.http.auth.{UsernamePasswordCredentials, AuthScope}
import org.apache.http.client.CredentialsProvider
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{HttpGet, CloseableHttpResponse}
import org.apache.http.impl.client.{BasicCredentialsProvider, CloseableHttpClient, HttpClientBuilder}

import scalaz._
import Scalaz._


/**
 * Some code adapted from Stack Overflow: http://stackoverflow.com/questions/6024376/apache-httpcomponents-httpclient-timeout
 */

object BusDataSource extends Iterator[String] {
  
  def busDataSource: Option[BusDataSource] = new BusDataSource
  
  def refreshDataSource = 

  override def hasNext: Boolean = currentDataSource.hasNext

  override def next(): String = currentDataSource.next
}

private class BusDataSource (config: DataSourceConfig) extends Iterator[String] with StrictLogging {

  private val httpRequestConfig = buildHttpRequestConfig(config.timeout)
  private val httpAuthScope = buildAuthScope(config.authScopeURL, config.authScopePort)
  private val httpCredentialsProvider = buildHttpCredentialsProvider(config.username, config.password, httpAuthScope)
  private val httpClient: CloseableHttpClient = buildHttpClient(httpRequestConfig, httpCredentialsProvider)
  private val httpResponse: CloseableHttpResponse = getHttpResponse(httpClient, config.sourceUrl)
  private val streamIterator:Iterator[String] = getStreamIterator(httpResponse)

  override def hasNext: Boolean = streamIterator.hasNext

  override def next() = streamIterator.next()
  
  def getStreamIterator(httpResponse: CloseableHttpResponse) = {
    httpResponse.getStatusLine.getStatusCode match {
      case HttpStatus.SC_OK =>
          val br = new BufferedReader(new InputStreamReader(httpResponse.getEntity.getContent))
          Stream.continually(br.readLine()).takeWhile(_ != null).drop(config.linesToDisregard).iterator
      case otherStatus: Int => 
          logger.debug(s"Error getting Stream Iterator. Http Status Code: $otherStatus")
          throw new IllegalStateException("Unable to retrieve input stream")
    }
  }

    def getHttpResponse(client: CloseableHttpClient, url: String): CloseableHttpResponse = {
      val httpGet = new HttpGet(url)
      client.execute(httpGet)
    }

    def buildHttpClient(requestConfig: RequestConfig, credentialsProvider: CredentialsProvider): CloseableHttpClient = {
      val client = HttpClientBuilder.create()
      client.setDefaultRequestConfig(requestConfig)
      client.setDefaultCredentialsProvider(credentialsProvider)
      client.build()
    }

    def buildHttpRequestConfig(connectionTimeout: Int): RequestConfig = {
      val requestBuilder = RequestConfig.custom()
      requestBuilder.setConnectionRequestTimeout(connectionTimeout)
      requestBuilder.setConnectTimeout(connectionTimeout)
      requestBuilder.build()
    }

    def buildHttpCredentialsProvider(userName: String, password: String, authScope: AuthScope): BasicCredentialsProvider = {
      val credentialsProvider = new BasicCredentialsProvider()
      val credentials = new UsernamePasswordCredentials(userName, password)
      credentialsProvider.setCredentials(authScope, credentials)
      credentialsProvider
    }

    def buildAuthScope(authScopeUrl: String, authScopePort: Int): AuthScope = {
      new AuthScope(authScopeUrl, authScopePort)
    }   
  
}