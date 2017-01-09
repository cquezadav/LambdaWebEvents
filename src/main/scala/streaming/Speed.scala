package streaming

import scala.reflect.runtime.universe

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.joda.time.DateTime

import config.ApplicationSettings
import config.InitializeApplication
import domain.EventsPerLocation
import utils.SparkUtils

object Speed extends App {

  InitializeApplication.connectStreaming()
  InitializeApplication.connectSpark()
  val streaming = SparkUtils.getStreamingContext
  val spark = SparkUtils.getSparkSession

  val kafkaTopic = ApplicationSettings.KafkaConfig.kafkaTopic
  val kafkaHost = ApplicationSettings.KafkaConfig.kafkaHost
  val kafkaPort = ApplicationSettings.KafkaConfig.kafkaPort

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> s"$kafkaHost:$kafkaPort",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "grupo_uno",
    "auto.offset.reset" -> "latest",
    "max.poll.records" -> "5000",
    "fetch.min.bytes" -> "1000",
    "enable.auto.commit" -> (false: java.lang.Boolean))

  @transient val messages = KafkaUtils.createDirectStream[String, String](streaming, PreferConsistent, Subscribe[String, String](Set(kafkaTopic), kafkaParams))

  @transient def toStringRDD(input: RDD[String]) = {
    @transient val res = input.flatMap { x => Some(x.toString) }
    res
  }

  def toEventsPerLocationDF(input: RDD[String]) = {
    spark.createDataFrame(
      spark.read.json(input).rdd.map { x =>
        var messageDate = new DateTime(x.getAs[Long]("timestamp")).toDateTime()
        var year = messageDate.toString("yyyy").toInt
        var month = messageDate.toString("MM").toInt
        var day = messageDate.toString("dd").toInt
        var hour = messageDate.toString("HH").toInt
        var minutes = messageDate.toString("mm").toInt
        val location = x.getAs[String]("location")
        EventsPerLocation(year, Some(month), Some(day), Some(hour), location)
      }).groupBy("year", "month", "day", "hour", "location").count()
  }

  @transient val mes = messages.map(record => record.value().toString)
  @transient val tra = mes.transform(record => toStringRDD(record))

  val keyspace = ApplicationSettings.SchemaConfig.keyspace
  val eventsPerLocationPerHourSpeedTable = ApplicationSettings.SchemaConfig.eventsPerLocationPerHourSpeedTable

  @transient val tab = tra.foreachRDD(x => toEventsPerLocationDF(x).write.format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> eventsPerLocationPerHourSpeedTable, "keyspace" -> keyspace))
    .mode(SaveMode.Append).save())

  streaming.start()
  streaming.awaitTermination()
}

