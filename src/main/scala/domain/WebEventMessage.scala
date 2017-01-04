package domain

case class WebEventMessage(messageId: Long, timestamp: Long, visitOrigin: String, location: String, department: String, productId: Long,
                           quantity: Int, action: String, transactionId: Long, paymentType: String, shipmentType: String)

case class LocationsPerHourBatch(year: Int, month: Int, day: Int, hour: Int, location: String, count: Long)