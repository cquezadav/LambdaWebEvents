package config

import scala.collection.JavaConversions
import scala.collection.JavaConversions.asScalaBuffer

import processor.DatabaseOperations
import utils.CassandraUtils
import com.datastax.driver.core.utils.UUIDs
import java.net.InetAddress

object InitializeApplication {

  private val nodes = ApplicationSettings.CassandraConfig.cassandraNodes
  private val inets = nodes.map(InetAddress.getByName).toList
  private val port = 9042
  
  def connectToCassandra() = {
    CassandraUtils.connect(inets, port)
  }
  
  def demo() = {
    try {

      val cql = "select * from system.schema_keyspaces ;"
      val resultSet = CassandraUtils.getSession().execute(cql)
      val itr = JavaConversions.asScalaIterator(resultSet.iterator)
      itr.foreach(row => {
        val keyspace_name = row.getString("keyspace_name")
        val strategy_options = row.getString("strategy_options")
        println(s"$keyspace_name $strategy_options")
      })

    } catch {
      case e: Exception => println("Got this unknown exception: " + e)
    }
  }

  def main(args: Array[String]) {
    //val keyspace = "system"

    //demo()
    DatabaseOperations.insertRawWebEvent(1, 1, 1, 1, 1, UUIDs.timeBased(), 1, 1, "1", "1", "1", 1, 1, "1", 1, "1", "1")

  }
}