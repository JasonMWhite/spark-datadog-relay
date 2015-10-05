package org.apache.spark

import github.gphat.datadog._
import org.apache.spark.scheduler._

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
}
