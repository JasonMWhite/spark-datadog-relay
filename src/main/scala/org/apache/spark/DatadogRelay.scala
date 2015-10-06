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

import com.timgroup.statsd.NonBlockingStatsDClient;
import org.apache.spark.scheduler._
import org.apache.spark.executor._
import scala.concurrent.ExecutionContext.Implicits.global

class DatadogRelay(conf: SparkConf) extends SparkFirehoseListener {
  val statsd: NonBlockingStatsDClient = new NonBlockingStatsDClient("spark", "localhost", 8125)
  
  def taskBaseMetrics(e: SparkListenerTaskEnd): Unit = {
    statsd.incrementCounter("firehose.taskEnded")
    statsd.recordExecutionTime("firehose.taskDuration", e.taskInfo.duration)
    if (!e.taskInfo.successful) statsd.incrementCounter("firehose.taskFailed")
    statsd.count("firehose.taskDiskBytesSpilled", e.taskMetrics.diskBytesSpilled)
    statsd.count("firehose.taskMemoryBytesSpilled", e.taskMetrics.memoryBytesSpilled)
    statsd.recordExecutionTime("firehose.taskExecutorRunTime", e.taskMetrics.executorRunTime)
    statsd.recordExecutionTime("firehose.taskResultSerializationTime", e.taskMetrics.resultSerializationTime)
    statsd.count("firehose.taskBytesSentToDriver", e.taskMetrics.resultSize)
  }
  
  def taskInputMetrics(metrics: InputMetrics): Unit = {
    statsd.count("firehose.taskInputBytesRead", metrics.bytesRead)
    statsd.count("firehose.taskInputRecordsRead", metrics.recordsRead)
  }
  
  def taskShuffleReadMetrics(metrics: ShuffleReadMetrics): Unit = {
    statsd.count("firehose.taskShuffleBytesRead", metrics.totalBytesRead)
    statsd.count("firehose.taskShuffleRecordsRead", metrics.recordsRead)
  }
  
  def taskShuffleWriteMetrics(metrics: ShuffleWriteMetrics): Unit = {
    statsd.count("firehose.taskShuffleBytesWritten", metrics.shuffleBytesWritten)
    statsd.count("firehose.taskShuffleRecordsWritten", metrics.shuffleRecordsWritten)
  }
  
  def taskOutputMetrics(metrics: OutputMetrics): Unit = {
    statsd.count("firehose.taskOutputBytesWritten", metrics.bytesWritten)
    statsd.count("firehose.taskOutputRecordsWritten", metrics.recordsWritten)
  }
  
  override def onEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SparkListenerApplicationStart =>
        statsd.incrementCounter("firehose.applicationStarted")
      case e: SparkListenerApplicationEnd =>
        statsd.incrementCounter("firehose.applicationEnded")
      case e: SparkListenerJobStart =>
        statsd.incrementCounter("firehose.jobStarted")
      case e: SparkListenerJobEnd =>
        statsd.incrementCounter("firehose.jobEnded")
      case e: SparkListenerStageSubmitted =>
        e.stageInfo.submissionTime.foreach { _ =>
          statsd.incrementCounter("firehose.stageStarted")
        }
      case e: SparkListenerStageCompleted =>
        e.stageInfo.completionTime.foreach { completionTime =>
          statsd.incrementCounter("firehose.stageEnded")
          e.stageInfo.submissionTime.foreach { submissionTime =>
            statsd.recordExecutionTime("firehose.stageProcessingTime", completionTime - submissionTime)
          }
          statsd.recordGaugeValue("firehose.stageRetryCount", e.stageInfo.attemptId)
          e.stageInfo.failureReason.foreach { _ =>
            statsd.incrementCounter("firehose.stageFailed")
          }
        }
      case e: SparkListenerTaskStart =>
        statsd.incrementCounter("firehose.taskStarted")
        statsd.recordGaugeValue("firehose.taskRetryCount", e.taskInfo.attempt)
      case e: SparkListenerTaskEnd =>
        taskBaseMetrics(e)
        e.taskMetrics.inputMetrics.foreach { m => taskInputMetrics(m) }
        e.taskMetrics.shuffleReadMetrics.foreach { m => taskShuffleReadMetrics(m) }
        e.taskMetrics.shuffleWriteMetrics.foreach { m => taskShuffleWriteMetrics(m) }
        e.taskMetrics.outputMetrics.foreach { m => taskOutputMetrics(m) }
      case e: SparkListenerExecutorAdded =>
        statsd.incrementCounter("firehose.executorAdded")
        statsd.count("firehose.executorCoresAdded", e.executorInfo.totalCores)
      case e: SparkListenerExecutorRemoved =>
        statsd.incrementCounter("firehose.executorRemoved")
      case e: SparkListenerBlockManagerAdded =>
        statsd.incrementCounter("firehose.blockManagerAdded")
      case e: SparkListenerBlockManagerRemoved =>
        statsd.incrementCounter("firehose.blockManagerRemoved")
      case _ =>
        None
    }
  }
}
