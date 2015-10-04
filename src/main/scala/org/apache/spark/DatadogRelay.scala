package org.apache.spark

import java.io.{Writer, OutputStreamWriter}
import java.net.{Socket, SocketException}
import java.nio.charset.Charset
import javax.net.SocketFactory

import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.apache.spark.scheduler.{SparkListenerExecutorMetricsUpdate, SparkListenerEvent}
import org.apache.spark.util.{Utils, JsonProtocol}
import org.json4s.JsonAST.{JObject, JNothing, JValue}

import github.gphat.datadog._
import org.apache.spark.scheduler.{SparkListenerEvent, SparkListenerApplicationStart, SparkListenerApplicationEnd}
import scala.concurrent.ExecutionContext.Implicits.global

class DatadogRelay(conf: SparkConf) extends SparkFirehoseListener {

  val apiKey = conf.get("spark.datadog.key", "xxx")
  val client = new Client(apiKey = apiKey, appKey = "xxx")
  
  override def onEvent(event: SparkListenerEvent): Unit = {
    val m: Option[Metric] = (event match {
      case e: SparkListenerApplicationStart =>        
        val seq = Seq((e.time / 1000, 1d))
        Some(Metric("spark.firehose.applicationsStarted", seq, Some("counter"), None, None))
      case e: SparkListenerApplicationEnd =>
        val seq = Seq((e.time / 1000, 1d))
        Some(Metric("spark.firehose.applicationsEnded", seq, Some("counter"), None, None))
      case _ => None
    })
    
    m match {
      case Some(event) =>
        client.addMetrics(Seq(event))
      case None => Unit
    }
  }
}
