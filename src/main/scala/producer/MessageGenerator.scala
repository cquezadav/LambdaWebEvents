package producer;

import scala.util.Random

import com.datastax.driver.core.utils.UUIDs

import Constants.Actions
import Constants.Departments
import Constants.PaymentType
import Constants.ShipmentType
import Constants.UserLocations
import Constants.VisitOrigins
import domain.WebEventMessage

object MessageGenerator {
  
  var counter : Long = 0;
  def generate: WebEventMessage = {
    val random = new Random()
    val timestamp = System.currentTimeMillis()
    val visitOrigin = VisitOrigins(random.nextInt(VisitOrigins.size))
    val location = UserLocations(random.nextInt(UserLocations.size))
    val department = Departments(random.nextInt(Departments.size))
    val departmentId = department._1
    val productId = departmentId + random.nextInt(4998)
    val action = Actions(random.nextInt(Actions.size))
    val transactionId = UUIDs.timeBased().timestamp()
    val quantity = if (action.equals("click") || action.equals("save for later")) 0 else random.nextInt(20)
    val paymentType = if (action.equals("click") || action.equals("save for later")) "none" else PaymentType(random.nextInt(PaymentType.size))
    val shipmentType = if (action.equals("click") || action.equals("save for later")) "none" else ShipmentType(random.nextInt(ShipmentType.size))
    counter += 1
    WebEventMessage(counter, timestamp, visitOrigin, location, department._2, productId, quantity, action, transactionId, paymentType, shipmentType)
  }

}
