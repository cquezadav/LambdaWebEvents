package batch

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

//import com.datastax.driver.core.Row
import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.toSparkContextFunctions
import com.datastax.spark.connector.SomeColumns
import org.apache.spark.sql.Row
import domain.LocationsPerHourBatch
import config.ApplicationSettings.SparkConfig
import config.ApplicationSettings.CassandraConfig

object Batch extends App {

  val sparkHost = SparkConfig.sparkHost
  val sparkPort = SparkConfig.sparkPort
  val cassandraHost = CassandraConfig.cassandraNodes.get(0)
  
  val sparkConf = new SparkConf(true)
    .setAppName("WebEventsApp")
    .setMaster(s"spark://$sparkHost:$sparkPort")
    .set("spark.cassandra.connection.host", cassandraHost)
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  val sc = spark.sparkContext
//  sc.addJar("/Users/carlos.quezada/.ivy2/cache/com.datastax.spark/spark-cassandra-connector_2.11/jars/spark-cassandra-connector_2.11-2.0.0-M3.jar")
//  sc.addJar("/Users/carlos.quezada/.ivy2/cache/joda-time/joda-time/jars/joda-time-2.8.1.jar")
//  sc.addJar("/Users/carlos.quezada/Personal/workspace/WebEvents/Domain.jar")
//  sc.addJar("/Users/carlos.quezada/Personal/workspace/WebEvents/Batch.jar")
  val keyspace = ""
  val tableName = ""

  val sqlContext = new SQLContext(sc)

  //val test_spark_rdd = sc.cassandraTable("web_events_analysis", "raw_events_data")
  //println(test_spark_rdd.first())  
  //test_spark_rdd.select("year").collect.foreach(println)
  //test_spark_rdd.groupBy ({ x => "minutes" })

  val df = sqlContext
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "raw_events_data", "keyspace" -> "web_events_analysis"))
    .load()

  //df.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "locations_per_hour_batch", "keyspace" -> "web_events_analysis")).save()
  //println(df.count())
  //df.show
  val locations = df.groupBy("year", "month", "day", "hour", "location").count().rdd


  val mapBatchQuery: RDD[LocationsPerHourBatch] = locations.map { x: Row =>
    val year = x.getAs[Int]("year")
    val month = x.getAs[Int]("month")
    val day = x.getAs[Int]("day")
    val hour = x.getAs[Int]("hour")
    val location = x.getAs[String]("location")
    val count = x.getAs[Long]("count")
    LocationsPerHourBatch(year, month, day, hour, location, count)
  }

  mapBatchQuery.saveToCassandra("web_events_analysis", "locations_per_hour_batch", SomeColumns("year", "month", "day", "hour", "location", "count"))

}
