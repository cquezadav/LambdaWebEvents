package config

import com.typesafe.config.ConfigFactory

object ApplicationSettings {
  private val config = ConfigFactory.load()
  private val rootConfig = config.getConfig("webEvents")

  object ApplicationConfig {
    lazy val applicationName = rootConfig.getString("applicationName")
  }

  object CassandraConfig {
    private val cassandraConfig = rootConfig.getConfig("cassandra")
    lazy val cassandraNodes = cassandraConfig.getStringList("nodes")
  }

  object SchemaConfig {
    private val schemaConfig = rootConfig.getConfig("schema")
    lazy val keyspace = schemaConfig.getString("keyspace")
    lazy val rawEventsDataTable = schemaConfig.getString("rawEventsDataTable")
    lazy val eventsPerLocationPerHourTable = schemaConfig.getString("eventsPerLocationPerHourTable")
    lazy val eventsPerLocationPerDayTable = schemaConfig.getString("eventsPerLocationPerDayTable")
    lazy val eventsPerLocationPerMonthTable = schemaConfig.getString("eventsPerLocationPerMonthTable")
    lazy val eventsPerLocationPerYearTable = schemaConfig.getString("eventsPerLocationPerYearTable")
  }

  object KafkaConfig {
    private val kafkaConfig = rootConfig.getConfig("kafka")
    lazy val kafkaHost = kafkaConfig.getString("host")
    lazy val kafkaPort = kafkaConfig.getString("port")
    lazy val kafkaTopic = kafkaConfig.getString("topic")
  }

  object SparkConfig {
    private val sparkConfig = rootConfig.getConfig("spark")
    lazy val sparkHost = sparkConfig.getString("host")
    lazy val sparkPort = sparkConfig.getString("port")
  }
}
