package producer

import com.google.gson.GsonBuilder
import java.util.Properties
import org.apache.kafka.clients.producer.ProducerConfig
import config.ApplicationSettings
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import scala.util.Random

object MessagesProducer extends App {

  val props = new Properties()
  val host = ApplicationSettings.KafkaConfig.kafkaHost
  val port = ApplicationSettings.KafkaConfig.kafkaPort
  val topic = ApplicationSettings.KafkaConfig.kafkaTopic

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, s"$host:$port")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "webeventsproducer")

  val kafkaProducer: Producer[Nothing, String] = new KafkaProducer[Nothing, String](props)
  println(kafkaProducer.partitionsFor(topic))

  val random = new Random()
  while (true) {

    var message = MessageGenerator.generate
    val gson = new GsonBuilder().disableHtmlEscaping().create()
    val messageJson = gson.toJson(message)
    println(messageJson)
    val producerRecord = new ProducerRecord(topic, messageJson)
    kafkaProducer.send(producerRecord)
    //Thread.sleep(random.nextInt(50))
    Thread.sleep(1000)
  }

}