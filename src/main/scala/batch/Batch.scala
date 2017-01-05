package batch

import scala.reflect.runtime.universe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.toNamedColumnRef
import com.datastax.spark.connector.toRDDFunctions

import config.ApplicationSettings.SchemaConfig
import config.InitializeApplication
import domain.LocationsPerHourBatch
import utils.SparkUtils

object Batch extends App {

  InitializeApplication.connectSpark()
  val spark = SparkUtils.getSparkSession
  val keyspace = SchemaConfig.keyspace
  val rawDataDF = spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "raw_events_data", "keyspace" -> "web_events_analysis"))
    .load()

  processEventsPerLocationPerHour
  processEventsPerLocationPerDay
  processEventsPerLocationPerMonth
  processEventsPerLocationPerYear

  def processEventsPerLocationPerHour = {
    val eventsPerLocationPerHourTable = SchemaConfig.eventsPerLocationPerHourTable
    val eventsPerLocationPerHourDF = rawDataDF.groupBy("year", "month", "day", "hour", "location").count()
    val eventsPerLocationPerHourRDD: RDD[LocationsPerHourBatch] = eventsPerLocationPerHourDF.rdd.map { x: Row =>
      val year = x.getAs[Int]("year")
      val month = x.getAs[Int]("month")
      val day = x.getAs[Int]("day")
      val hour = x.getAs[Int]("hour")
      val location = x.getAs[String]("location")
      val count = x.getAs[Long]("count")
      LocationsPerHourBatch(year, Some(month), Some(day), Some(hour), location, count)
    }

    eventsPerLocationPerHourRDD.saveToCassandra(keyspace, eventsPerLocationPerHourTable, SomeColumns("year", "month", "day", "hour", "location", "count"))
  }

  def processEventsPerLocationPerDay = {
    val eventsPerLocationPerDayTable = SchemaConfig.eventsPerLocationPerDayTable
    val eventsPerLocationPerDayDF = rawDataDF.groupBy("year", "month", "day", "location").count()
    val eventsPerLocationPerDayRDD: RDD[LocationsPerHourBatch] = eventsPerLocationPerDayDF.rdd.map { x: Row =>
      val year = x.getAs[Int]("year")
      val month = x.getAs[Int]("month")
      val day = x.getAs[Int]("day")
      val location = x.getAs[String]("location")
      val count = x.getAs[Long]("count")
      LocationsPerHourBatch(year, Some(month), Some(day), None, location, count)
    }

    eventsPerLocationPerDayRDD.saveToCassandra(keyspace, eventsPerLocationPerDayTable, SomeColumns("year", "month", "day", "location", "count"))
  }

  def processEventsPerLocationPerMonth = {
    val eventsPerLocationPerMonthTable = SchemaConfig.eventsPerLocationPerMonthTable
    val eventsPerLocationPerMonthDF = rawDataDF.groupBy("year", "month", "location").count()
    val eventsPerLocationPerMonthRDD: RDD[LocationsPerHourBatch] = eventsPerLocationPerMonthDF.rdd.map { x: Row =>
      val year = x.getAs[Int]("year")
      val month = x.getAs[Int]("month")
      val location = x.getAs[String]("location")
      val count = x.getAs[Long]("count")
      LocationsPerHourBatch(year, Some(month), None, None, location, count)
    }

    eventsPerLocationPerMonthRDD.saveToCassandra(keyspace, eventsPerLocationPerMonthTable, SomeColumns("year", "month", "location", "count"))
  }

  def processEventsPerLocationPerYear = {
    val eventsPerLocationPerYearTable = SchemaConfig.eventsPerLocationPerYearTable
    val eventsPerLocationPerYearDF = rawDataDF.groupBy("year", "month", "location").count()
    val eventsPerLocationPerYearRDD: RDD[LocationsPerHourBatch] = eventsPerLocationPerYearDF.rdd.map { x: Row =>
      val year = x.getAs[Int]("year")
      val month = x.getAs[Int]("month")
      val location = x.getAs[String]("location")
      val count = x.getAs[Long]("count")
      LocationsPerHourBatch(year, Some(month), None, None, location, count)
    }

    eventsPerLocationPerYearRDD.saveToCassandra(keyspace, eventsPerLocationPerYearTable, SomeColumns("year", "location", "count"))
  }

}
