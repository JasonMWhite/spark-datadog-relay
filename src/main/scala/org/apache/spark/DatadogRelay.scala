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
import org.apache.spark.scheduler._
import scala.concurrent.ExecutionContext.Implicits.global

class DatadogRelay(conf: SparkConf) extends SparkFirehoseListener {

  val apiKey = conf.get("spark.datadog.key", "xxx")
  val client = new Client(apiKey = apiKey, appKey = "xxx")
  
  def generateCounterSequence(name: String, time: Long, amount: Double = 1d): Seq[Metric] = {
    Seq(generateCounter(name, time, amount = amount))
  }
  
  def generateCounter(name: String, time: Long, amount: Double = 1d): Metric = {
    Metric(name, Seq((time, amount)), Some("counter"), None, None)
  }
  
  override def onEvent(event: SparkListenerEvent): Unit = {
    val metrics: Option[Seq[Metric]] = (event match {
      case e: SparkListenerApplicationStart =>
        Some(generateCounterSequence("spark.firehose.applicationStarted", e.time / 1000))
      case e: SparkListenerApplicationEnd =>
        Some(generateCounterSequence("spark.firehose.applicationEnded", e.time / 1000))
      case e: SparkListenerJobStart =>
        Some(generateCounterSequence("spark.firehose.jobStarted", e.time / 1000))
      case e: SparkListenerJobEnd =>
        Some(generateCounterSequence("spark.firehose.jobEnded", e.time / 1000))
      case e: SparkListenerStageSubmitted =>
        e.stageInfo.submissionTime.map { time =>
          generateCounterSequence("spark.firehose.stageStarted", time / 1000)
        }
      case e: SparkListenerStageCompleted =>
        e.stageInfo.completionTime.flatMap { completionTime =>
          val processingMetric: Option[Metric] = e.stageInfo.submissionTime.map { submissionTime =>
            generateCounter("spark.firehose.stageProcessingTime", completionTime / 1000, amount = (completionTime - submissionTime).doubleValue() / 1000)
          }
          val retryMetric: Option[Metric] = Some(generateCounter("spark.firehose.stageRetries", completionTime / 1000, amount = e.stageInfo.attemptId.toDouble))
          val stageCompletionMetric = Some(generateCounter("spark.firehose.stageEnded", completionTime / 1000))
          Some(Seq(processingMetric, retryMetric, stageCompletionMetric).flatten)
        }
      case _ => None
    })
    
    metrics.foreach { seq =>
      seq.foreach { event =>
        event.points.foreach({ point =>
          println("Event name: " + event.name)
          println("Event timestamp: " + point._1.toString())
          println("Event measure: " + point._2.toString())
        })
      }
      client.addMetrics(seq)
    }
  }
}
