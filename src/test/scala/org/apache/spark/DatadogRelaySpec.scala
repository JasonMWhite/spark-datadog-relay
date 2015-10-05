package org.apache.spark

import github.gphat.datadog._
import org.apache.spark.scheduler._
import org.apache.spark.executor._

class DatadogRelaySpec extends UnitSpec {
  val conf = new SparkConf()
  val relay = new DatadogRelay(conf)
  
  test("Metrics should have value of 1 by default") {
    val testMetric = relay.generateCounter("foo", 10)
    assert(testMetric.name == "foo")
    assert(testMetric.points.length == 1)
    assert(testMetric.points(0)._1 == 10)
    assert(testMetric.points(0)._2 == 1)
  }
  
  test("Metrics should have value set explicitly") {
    val testMetric = relay.generateCounter("bar", 10, amount = 15.3)
    assert(testMetric.name == "bar")
    assert(testMetric.points.length == 1)
    assert(testMetric.points(0)._1 == 10)
    assert(testMetric.points(0)._2 == 15.3)
  }
  
  test("Sequence generator should use default value") {
    val testMetricSequence = relay.generateCounterSequence("foo", 5)
    assert(testMetricSequence.length == 1)
    assert(testMetricSequence(0).points(0)._2 == 1)
  }
  
  test("Sequence generator should use explicit value") {
    val testMetricSequence = relay.generateCounterSequence("foo", 5, amount = 10.1)
    assert(testMetricSequence.length == 1)
    assert(testMetricSequence(0).points(0)._2 == 10.1)
  }
  
  test("stageCompletedMetrics provides 3 metrics on successful task") {
    val stageInfo = new StageInfo(0, 2, "Test Stage", 128, Seq(), Seq(), "No Details", Seq())
    stageInfo.submissionTime = Some(1000)
    stageInfo.completionTime = Some(5000)
    val e = new SparkListenerStageCompleted(stageInfo)
    
    val resultSequence = relay.stageCompletedMetrics(e)
    assert(resultSequence.head.length == 3)
    assert(resultSequence.head(0) == Metric("spark.firehose.stageProcessingTime", Seq((5, 4d)), Some("counter"), None, None))
    assert(resultSequence.head(1) == Metric("spark.firehose.stageRetryCount", Seq((5, 2d)), Some("counter"), None, None))
    assert(resultSequence.head(2) == Metric("spark.firehose.stageEnded", Seq((5, 1d)), Some("counter"), None, None))
  }
  
  test("stageCompletedMetrics provides 4 metrics on failed task") {
    val stageInfo = new StageInfo(0, 2, "Test Stage", 128, Seq(), Seq(), "No Details", Seq())
    stageInfo.submissionTime = Some(1000)
    stageInfo.completionTime = Some(5000)
    stageInfo.failureReason = Some("Something happened")
    val e = new SparkListenerStageCompleted(stageInfo)
    
    val resultSequence = relay.stageCompletedMetrics(e)
    assert(resultSequence.head.length == 4)
    assert(resultSequence.head(3) == Metric("spark.firehose.stageFailed", Seq((5, 1d)), Some("counter"), None, None))
  }
  
  test("taskInputMetrics provides 2 metrics") {
    val inputMetrics = InputMetrics(DataReadMethod.Hadoop)
    inputMetrics.incBytesRead(1000)
    inputMetrics.incRecordsRead(50)
    
    val resultSequence = relay.taskInputMetrics(20, Some(inputMetrics))
    assert(resultSequence.length == 2)
    assert(resultSequence(0) == Metric("spark.firehose.taskInputBytesRead", Seq((20, 1000)), Some("counter"), None, None))
    assert(resultSequence(1) == Metric("spark.firehose.taskInputRecordsRead", Seq((20, 50)), Some("counter"), None, None))
  }
  
  test("taskShuffleReadMetrics provides 2 metrics") {
    val shuffleReadMetrics = new ShuffleReadMetrics()
    shuffleReadMetrics.incRemoteBytesRead(1000)
    shuffleReadMetrics.incRecordsRead(50)
    
    val resultSequence = relay.taskShuffleReadMetrics(20, Some(shuffleReadMetrics))
    assert(resultSequence.length == 2)
    assert(resultSequence(0) == Metric("spark.firehose.taskShuffleBytesRead", Seq((20, 1000)), Some("counter"), None, None))
    assert(resultSequence(1) == Metric("spark.firehose.taskShuffleRecordsRead", Seq((20, 50)), Some("counter"), None, None))
  }
  
  test("taskShuffleWriteMetrics provides 2 metrics") {
    val shuffleWriteMetrics = new ShuffleWriteMetrics()
    shuffleWriteMetrics.incShuffleBytesWritten(1000)
    shuffleWriteMetrics.incShuffleRecordsWritten(50)
    
    val resultSequence = relay.taskShuffleWriteMetrics(20, Some(shuffleWriteMetrics))
    assert(resultSequence.length == 2)
    assert(resultSequence(0) == Metric("spark.firehose.taskShuffleBytesWritten", Seq((20, 1000)), Some("counter"), None, None))
    assert(resultSequence(1) == Metric("spark.firehose.taskShuffleRecordsWritten", Seq((20, 50)), Some("counter"), None, None))
  }
  
  test("taskOutputMetrics provides 2 metrics") {
    val outputMetrics = new OutputMetrics(DataWriteMethod.Hadoop)
    outputMetrics.setBytesWritten(1000)
    outputMetrics.setRecordsWritten(50)
    
    val resultSequence = relay.taskOutputMetrics(20, Some(outputMetrics))
    assert(resultSequence.length == 2)
    assert(resultSequence(0) == Metric("spark.firehose.taskOutputBytesWritten", Seq((20, 1000)), Some("counter"), None, None))
    assert(resultSequence(1) == Metric("spark.firehose.taskOutputRecordsWritten", Seq((20, 50)), Some("counter"), None, None))
  }
  
  test("taskEndBaseMetrics provides 7 metrics on success") {
    val taskInfo = new TaskInfo(0, 0, 1, 1000, "abcd-id", "host.shopify.io", TaskLocality.NODE_LOCAL, false)
    taskInfo.finishTime = 5000
    val taskMetrics = new TaskMetrics()
    taskMetrics.incDiskBytesSpilled(1000)
    taskMetrics.incMemoryBytesSpilled(2000)
    taskMetrics.setExecutorRunTime(3000)
    taskMetrics.setResultSerializationTime(1000)
    taskMetrics.setResultSize(500)
    val e = SparkListenerTaskEnd(0, 3, "Foo Type", Success, taskInfo, taskMetrics)
    
    val resultSequence = relay.taskEndBaseMetrics(5, e)
    assert(resultSequence.length == 7)
    assert(resultSequence(0) == Metric("spark.firehose.taskEnded", Seq((5, 1)), Some("counter"), None, None))
    assert(resultSequence(1) == Metric("spark.firehose.taskDuration", Seq((5, 4)), Some("counter"), None, None))
    assert(resultSequence(2) == Metric("spark.firehose.taskDiskBytesSpilled", Seq((5, 1000)), Some("counter"), None, None))
    assert(resultSequence(3) == Metric("spark.firehose.taskMemoryBytesSpilled", Seq((5, 2000)), Some("counter"), None, None))
    assert(resultSequence(4) == Metric("spark.firehose.taskExecutorRunTime", Seq((5, 3)), Some("counter"), None, None))
    assert(resultSequence(5) == Metric("spark.firehose.taskResultSerializationTime", Seq((5, 1)), Some("counter"), None, None))
    assert(resultSequence(6) == Metric("spark.firehose.taskBytesSentToDriver", Seq((5, 500)), Some("counter"), None, None))
  }
}
