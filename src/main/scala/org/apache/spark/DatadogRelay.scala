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
  
  def stageCompletedMetrics(e: SparkListenerStageCompleted): Option[Seq[Metric]] = {
    e.stageInfo.completionTime.flatMap { completionTime =>
      val completionSeconds = completionTime / 1000
      val processingMetric: Option[Metric] = e.stageInfo.submissionTime.map { submissionTime =>
        generateCounter("spark.firehose.stageProcessingTime", completionTime / 1000, amount = (completionSeconds - submissionTime / 1000).doubleValue())
      }
      val retryMetric: Option[Metric] = Some(generateCounter("spark.firehose.stageRetryCount", completionSeconds, amount = e.stageInfo.attemptId.toDouble))
      val completionMetric = Some(generateCounter("spark.firehose.stageEnded", completionSeconds))
      val failureMetric = e.stageInfo.failureReason.map { _ =>
        generateCounter("spark.firehose.stageFailed", completionSeconds)
      }
      
      Some(Seq(processingMetric, retryMetric, completionMetric, failureMetric).flatten)
    }
  }
  
  def taskEndMetrics(e: SparkListenerTaskEnd): Option[Seq[Metric]] = {
    val finishSeconds = e.taskInfo.finishTime / 1000

    val taskMetrics: Seq[Metric] = Seq(
      Some(generateCounter("spark.firehose.taskEnded", finishSeconds)),
      Some(generateCounter("spark.firehose.taskDuration", finishSeconds, amount = e.taskInfo.duration)),
      if (e.taskInfo.successful) None else Some(generateCounter("spark.firehose.taskFailed", finishSeconds)),
      Some(generateCounter("spark.firehose.taskDiskBytesSpilled", finishSeconds, amount = e.taskMetrics.diskBytesSpilled)),
      Some(generateCounter("spark.firehose.taskMemoryBytesSpilled", finishSeconds, amount = e.taskMetrics.memoryBytesSpilled)),
      Some(generateCounter("spark.firehose.taskExecutorRunTime", finishSeconds, amount = e.taskMetrics.executorRunTime)),
      Some(generateCounter("spark.firehose.taskResultSerializationTime", finishSeconds, amount = e.taskMetrics.resultSerializationTime)),
      Some(generateCounter("spark.firehose.taskBytesSentToDriver", finishSeconds, amount = e.taskMetrics.resultSize))
    ).flatten
    
    val taskInputMetrics: Seq[Metric] = e.taskMetrics.inputMetrics.map { inputMetrics =>
      Seq(
        generateCounter("spark.firehose.taskInputBytesRead", finishSeconds, amount = inputMetrics.bytesRead),
        generateCounter("spark.firehose.taskInputRecordsRead", finishSeconds, amount = inputMetrics.recordsRead)
      )
    }.getOrElse(Seq())
    
    val taskShuffleReadMetrics: Seq[Metric] = e.taskMetrics.shuffleReadMetrics.map { shuffleReadMetrics =>
      Seq(
        generateCounter("spark.firehose.taskShuffleBytesRead", finishSeconds, amount = shuffleReadMetrics.totalBytesRead),
        generateCounter("spark.firehose.taskShuffleRecordsRead", finishSeconds, amount = shuffleReadMetrics.recordsRead)
      )
    }.getOrElse(Seq())
    
    val taskShuffleWriteMetrics: Seq[Metric] = e.taskMetrics.shuffleWriteMetrics.map { shuffleWriteMetrics =>
      Seq(
        generateCounter("spark.firehose.taskShuffleBytesWritten", finishSeconds, amount = shuffleWriteMetrics.shuffleBytesWritten),
        generateCounter("spark.firehose.taskShuffleRecordsWritten", finishSeconds, amount = shuffleWriteMetrics.shuffleRecordsWritten)
      )
    }.getOrElse(Seq())
    
    val taskOutputMetrics: Seq[Metric] = e.taskMetrics.outputMetrics.map { outputMetrics =>
      Seq(
        generateCounter("spark.firehose.taskOutputBytesWritten", finishSeconds, amount = outputMetrics.bytesWritten),
        generateCounter("spark.firehose.taskOutputRecordsWritten", finishSeconds, amount = outputMetrics.recordsWritten)
      )
    }.getOrElse(Seq())
    
    Some(Seq(taskMetrics, taskInputMetrics, taskShuffleReadMetrics, taskShuffleWriteMetrics, taskOutputMetrics).flatten)
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
      case e: SparkListenerStageCompleted => stageCompletedMetrics(e)
      case e: SparkListenerTaskStart =>
        val launchSeconds = e.taskInfo.launchTime / 1000
        Some(Seq(
          generateCounter("spark.firehose.taskStarted", launchSeconds),
          generateCounter("spark.firehose.taskRetryCount", launchSeconds, amount = e.taskInfo.attempt)
        ))
      case e: SparkListenerTaskEnd => taskEndMetrics(e)
      case e: SparkListenerExecutorAdded =>
        val addedSeconds = e.time / 1000
        Some(Seq(
          generateCounter("spark.firehose.executorAdded", addedSeconds),
          generateCounter("spark.firehose.executorCoresAdded", addedSeconds, amount = e.executorInfo.totalCores)
        ))
      case e: SparkListenerExecutorRemoved =>
        Some(generateCounterSequence("spark.firehose.executorRemoved", e.time))
      case e: SparkListenerBlockManagerAdded =>
        Some(generateCounterSequence("spark.firehose.blockManagerAdded", e.time))
      case e: SparkListenerBlockManagerRemoved =>
        Some(generateCounterSequence("spark.firehose.blockManagerRemoved", e.time))
      case _ => None
    })
    
    metrics.foreach { seq => client.addMetrics(seq)}
  }
}
