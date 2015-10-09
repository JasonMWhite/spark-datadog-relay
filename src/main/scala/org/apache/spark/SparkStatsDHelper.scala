package org.apache.spark

import com.timgroup.statsd.{NonBlockingStatsDClient, StatsDClientException}

object SparkStatsDHelper {
  def tags(conf: SparkConf): List[String] = {
    val datadogTags = conf.get("spark.datadog.tags", "")
    if (datadogTags == "") List() else datadogTags.split(",").toList
  }
  
  def getClient(conf: SparkConf): Option[NonBlockingStatsDClient] = {
    try {
      Some(new NonBlockingStatsDClient(
          "spark",
          "localhost",
          8125,
          tags(conf).mkString(",")
      ))
    } catch {
      case ex: StatsDClientException => None
      case ex: Exception => throw ex
    }
  }
}
