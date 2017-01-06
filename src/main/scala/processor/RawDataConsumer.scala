package processor

import java.util.Collections
import java.util.Properties
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import scala.collection.JavaConversions.iterableAsScalaIterable

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.joda.time.DateTime

import com.datastax.driver.core.utils.UUIDs
import com.google.gson.Gson

import config.ApplicationSettings
import config.InitializeApplication
import domain.WebEventMessage

class RawDataConsumer {

  val host = ApplicationSettings.KafkaConfig.kafkaHost
  val port = ApplicationSettings.KafkaConfig.kafkaPort
  val topic = ApplicationSettings.KafkaConfig.kafkaTopic

  val props = createConsumerConfig(s"$host:$port", "group_1")
  val consumer = new KafkaConsumer[String, String](props)
  var executor: ExecutorService = null

  InitializeApplication.connectCassandra()

  def shutdown() = {
    if (consumer != null)
      consumer.close();
    if (executor != null)
      executor.shutdown();
  }

  def createConsumerConfig(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  def run() = {
    consumer.subscribe(Collections.singletonList(this.topic))

    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          val records = consumer.poll(1000)
          val gson = new Gson
          for (record <- records) {
            System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
            val webMessage = gson.fromJson(record.value(), classOf[WebEventMessage])
            println(webMessage)

            var messageDate = new DateTime(webMessage.timestamp).toDateTime()
            var year = messageDate.toString("yyyy").toInt
            var month = messageDate.toString("MM").toInt
            var day = messageDate.toString("dd").toInt
            var hour = messageDate.toString("HH").toInt
            var minutes = messageDate.toString("mm").toInt 
            println(s"$year - $month - $day - $hour - $minutes")

            DatabaseOperations.insertRawWebEvent(year, month, day, hour, minutes, UUIDs.timeBased(), webMessage.messageId, webMessage.timestamp,
              webMessage.visitOrigin, webMessage.location, webMessage.department, webMessage.productId, webMessage.quantity,
              webMessage.action, webMessage.transactionId, webMessage.paymentType, webMessage.shipmentType)
          }
        }
      }
    })
  }
}

object RawDataConsumer extends App {
  val example = new RawDataConsumer()
  example.run()
}
