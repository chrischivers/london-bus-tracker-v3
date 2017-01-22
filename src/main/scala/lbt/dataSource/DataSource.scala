package lbt.dataSource

import java.io.{BufferedReader, InputStreamReader}

import com.typesafe.scalalogging.StrictLogging
import fs2.Task
import lbt.{DataSourceConfig, LBTConfig}
import org.apache.http.HttpStatus
import org.apache.http.auth.{UsernamePasswordCredentials, AuthScope}
import org.apache.http.client.CredentialsProvider
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.config.RequestConfig.Builder
import org.apache.http.client.methods.{HttpGet, CloseableHttpResponse}
import org.apache.http.impl.client.{BasicCredentialsProvider, CloseableHttpClient, HttpClientBuilder}

import scalaz._
import Scalaz._


/**
 * Some code adapted from Stack Overflow: http://stackoverflow.com/questions/6024376/apache-httpcomponents-httpclient-timeout
 */

object DataSource extends StrictLogging {

  var currentHttpClient: Option[CloseableHttpClient] = None
  var currentHttpResponse: Option[CloseableHttpResponse] = None

  def getNewStream(config: DataSourceConfig): Exception \/ Stream[String] = {
    logger.info("Closing existing streams (if existing")

    if (currentHttpClient.isDefined) currentHttpClient.get.close()
    if (currentHttpResponse.isDefined) currentHttpResponse.get.close()

    logger.info("Opening new stream")

    val requestConfig = buildHttpRequestConfig(config.timeout)
    val authScope = buildAuthScope(config.authScopeURL, config.authScopePort)
    val credentialsProvider = buildHttpCredentialsProvider(config.username, config.password, authScope)
    val httpClient = buildHttpClient(requestConfig, credentialsProvider)
    val response = getHttpResponse(httpClient, config.sourceUrl)

    response.getStatusLine.getStatusCode match {
      case HttpStatus.SC_OK =>
        currentHttpClient = Some(httpClient)
        currentHttpResponse= Some(response)
        val br = new BufferedReader(new InputStreamReader(response.getEntity.getContent))
        \/.right(Stream.continually(br.readLine()).takeWhile(_ != null).drop(config.linesToDisregard))

      case error: Int =>
        response.close()
        httpClient.close()
        \/.left(new Exception(s"Error opening data stream. Error code: $error"))
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
}