package utils

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext

object SparkUtils {
  private var sparkSession: SparkSession = null

  def createSparkSession(appName: String, sparkHost: String, sparkPort: String, cassandraHost: String) = {
    val sparkConf = new SparkConf(true)
      .setAppName(appName)
      .setMaster(s"spark://$sparkHost:$sparkPort")
      .set("spark.cassandra.connection.host", cassandraHost)

    sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // TODO
    //val checkpointDirectory = "//checkpoint"
    //sparkSession.sparkContext.setCheckpointDir(checkpointDirectory)
  }

  def getSparkSession = {
    sparkSession
  }

  def getSparkContext = {
    sparkSession.sparkContext
  }

  def getSQLContext = {
    sparkSession.sqlContext
  }

  // TODO
  def getStreamingContext(streamingApp: (SparkContext, Duration) => StreamingContext, sc: SparkContext, batchDuration: Duration) = {
    val creatingFunc = () => streamingApp(sc, batchDuration)
    val ssc = sc.getCheckpointDir match {
      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
      case None                => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach(cp => ssc.checkpoint(cp))
    ssc
  }
}