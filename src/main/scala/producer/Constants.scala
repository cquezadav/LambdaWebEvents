package producer;

object Constants {

  val VisitOrigins = Seq("mobile", "mobile ad", "website", "website ad", "email ad", "text message ad", "alexa", "google home")
  val Departments = Seq((1, "TV & Video"), (5000, "Home Audio & Theater"), (10000, "Camera, Photo & Video"), (15000, "Cell Phones & Accessories"),
    (20000, "Headphones"), (25000, "Video Games"), (30000, "Bluetooth & Wireless Speakers"), (35000, "Car Electronics"), (40000, "Musical Instruments"),
    (45000, "Internet, TV and Phone Services"), (50000, "Wearable Technology"), (55000, "Electronics Showcase"), (60000, "Computers & Tablets"),
    (65000, "Monitors"), (70000, "Accessories"), (75000, "Networking"), (80000, "Drives & Storage"), (85000, "Computer Parts & Components"),
    (90000, "Software"), (95000, "Printers & Ink"), (100000, "Office & School Supplies"), (105000, "Trade In Your Electronics"))
  val Actions = Seq("click product", "click ad", "save for later", "purchase", "add to cart")
  val PaymentType = Seq("credit card", "gift card", "paypal", "visa checkout")
  val ShipmentType = Seq("shipment", "pickup")
  val UserLocations = {
    for {
      line <- scala.io.Source.fromURL(getClass.getResource("/us_states.csv")).getLines().toVector
      values = line.split(",").map(_.trim)
    } yield values(2)
  }

}