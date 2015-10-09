package org.apache.spark

import org.apache.spark.streaming.scheduler._
import com.timgroup.statsd.{NonBlockingStatsDClient, StatsDClientException}

class DatadogStreamingRelay(conf: SparkConf) extends StreamingListener {
  
  val statsdOption: Option[NonBlockingStatsDClient] = SparkStatsDHelper.getClient(conf)
  
  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) {
    statsdOption.foreach { statsd =>  
      statsd.incrementCounter("firehose.batchStarted")
    }
  }
  
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    statsdOption.foreach { statsd =>
      statsd.incrementCounter("firehose.batchCompleted")
      statsd.count("firehose.batchRecordsProcessed", batchCompleted.batchInfo.numRecords)
      batchCompleted.batchInfo.processingDelay.foreach { delay =>
        statsd.recordExecutionTime("firehose.batchProcessingDelay", delay)
      }
      batchCompleted.batchInfo.schedulingDelay.foreach { delay =>
        statsd.recordExecutionTime("firehose.batchSchedulingDelay", delay)
      }
      
      batchCompleted.batchInfo.processingDelay.foreach { delay =>
        statsd.recordExecutionTime("firehose.batchProcessingDelay", delay)
      }
      batchCompleted.batchInfo.totalDelay.foreach { delay =>
        statsd.recordExecutionTime("firehose.batchTotalDelay", delay)
      }
    }
  }
}