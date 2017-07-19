import com.fastdtw.dtw.FastDTW
import com.fastdtw.util.EuclideanDistance
import scala.math.exp

/**
  * Created by kiselev on 12/07/2017.
  */
object CheckinTimeSeries {

  def getTimeSeries(userCheckins: Iterable[Checkin]) = new ScalaTimeSeries(
    Option(userCheckins).toSeq.flatten
      .map(checkin => ScalaTimeSeriesItem(
        checkin.time.getTime,
        ScalaTimeSeriesPoint(Array(checkin.lat, checkin.lon))
      ))
  )

  def weight(leftUserCheckins: Iterable[Checkin], rightUserCheckins: Iterable[Checkin]): Double = {
    val leftTimeSeries = getTimeSeries(leftUserCheckins)
    val rightTimeSeries = getTimeSeries(rightUserCheckins)
    val distance = if (leftTimeSeries.size() <= 2 ||  rightTimeSeries.size() <= 2) Double.MaxValue
                   else FastDTW.compare(leftTimeSeries, rightTimeSeries, new EuclideanDistance).getDistance
    exp(-distance)
  }

}
