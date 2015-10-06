package org.apache.spark

import java.nio.charset.Charset
import com.timgroup.statsd.{NonBlockingStatsDClient, StatsDClientException, StatsDClientErrorHandler} 
import scala.reflect.runtime.{universe => ru}
import scala.reflect.runtime.universe._

package object tagUtils {
  implicit class StatsDClientWithTag(val client: NonBlockingStatsDClient)(implicit val tags: List[String]) {
    val originalMessageFor = {
      val m = ru.runtimeMirror(client.getClass.getClassLoader)
      val im = m.reflect(client)
      val symbol = ru.typeOf[NonBlockingStatsDClient].member(newTermName("messageFor"))
      
      // Method of interest is overridden: pick the one with 4 arguments
      val method = symbol.asTerm.alternatives.filter(m => m.asMethod.paramss.head.length == 4).head.asMethod
      im.reflectMethod(method)
    }
    
    def tagSuffix(): String = {
      if (tags.length == 0) "" else "|#" + tags.mkString(",")
    }
    
    def messageFor(aspect: String, value: String, `type`: String, sampleRate: Double): String = {
      originalMessageFor(aspect, value, `type`, sampleRate) + tagSuffix
    }
    
    def messageFor(aspect: String, value: String, `type`: String): String = {
      originalMessageFor(aspect, value, `type`, 1.0) + tagSuffix
    }
  }
}
