import java.sql.Timestamp
import org.joda.time.DateTime

case class Checkin(userId: Int, time: Timestamp,
                   lat: Double, lon: Double,
                   locationId: String)
  extends Serializable

object Checkin {
  lazy val fromArray: PartialFunction[Array[String], Checkin] = {
    case Array(userId: String, checkinTime: String,
               lat: String, lon: String,
               locationId: String) =>
      Checkin(userId.toInt, new Timestamp(DateTime.parse(checkinTime).getMillis),
        lat.toDouble, lon.toDouble,
        locationId)
  }
}
