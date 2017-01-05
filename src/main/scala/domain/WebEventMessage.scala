package domain

case class WebEventMessage(messageId: Long, timestamp: Long, visitOrigin: String, location: String, department: String, productId: Long,
                           quantity: Int, action: String, transactionId: Long, paymentType: String, shipmentType: String)

case class LocationsPerHourBatch(year: Int, month: Option[Int] = None, day: Option[Int] = None, hour: Option[Int] = None, location: String, count: Long)