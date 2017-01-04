package processor

import utils.CassandraUtils
import com.datastax.driver.core.exceptions.DriverException
import java.util.UUID

object DatabaseOperations {

  def insertRawWebEvent(year: Int, month: Int, day: Int, hour: Int, minutes: Int, eventId: UUID, messageId: Long, timestamp: Long, visitOrigin: String, location: String,
                        department: String, productId: Long, quantity: Int, action: String, transactionId: Long, paymentType: String, shipmentType: String) = {

    var cql = s"""INSERT INTO web_events_analysis.raw_events_data 
      (year, month, day, hour, minutes, event_id, message_id, timestamp, visit_origin, location, department, 
      product_id, quantity, action, transaction_id, payment_type, shipment_type)
      VALUES ($year, $month, $day, $hour, $minutes, $eventId, $messageId, $timestamp, '$visitOrigin', '$location', 
      '$department', $productId, $quantity, '$action', $transactionId, '$paymentType', '$shipmentType')"""

    println(cql)
    val session = CassandraUtils.getSession()
    try {
      session.execute(cql)
    } catch {
      case e: DriverException => println("Could not insert web event into cassandra: " + e)
      case e: Exception       => println("Got this unknown exception: " + e)
    }
  }
}