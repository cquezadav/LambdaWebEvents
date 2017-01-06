package streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import com.google.gson.Gson

import config.InitializeApplication
import utils.SparkUtils

object Speed extends App {

  InitializeApplication.connectStreaming()
  val streaming = SparkUtils.getStreamingContext

  val topic = "webevents"

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "69.164.212.142:32769",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "grupo_uno",
    "auto.offset.reset" -> "latest",
    "max.poll.records" -> "5000",
    "fetch.min.bytes" -> "1000",
    "enable.auto.commit" -> (false: java.lang.Boolean))

  val messages = KafkaUtils.createDirectStream(streaming, PreferConsistent, Subscribe[String, String](Set(topic), kafkaParams))

  val gson = new Gson
  val mes = messages.map(record => (record.value).toString())
  
  mes.print()
  
  //val ms = messages.map(record => (record.key, record.value))
  
  streaming.start()
  streaming.awaitTermination()
}