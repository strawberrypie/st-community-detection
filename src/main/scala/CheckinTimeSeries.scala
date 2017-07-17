import com.fastdtw.dtw.FastDTW
import com.fastdtw.util.EuclideanDistance


/**
  * Created by kiselev on 12/07/2017.
  */
object CheckinTimeSeries {

  def getTimeSeries(userCheckins: Iterable[Checkin]) = new ScalaTimeSeries(
    userCheckins
      .map(checkin => ScalaTimeSeriesItem(
        checkin.time.getTime,
        ScalaTimeSeriesPoint(Array(checkin.lat, checkin.lon)))
      ).toSeq
  )

  def metric(leftUserCheckins: Iterable[Checkin], rightUserCheckins: Iterable[Checkin]): Double = {
    val leftTimeSeries = getTimeSeries(leftUserCheckins)
    val rightTimeSeries = getTimeSeries(rightUserCheckins)
    if (leftTimeSeries.size() + rightTimeSeries.size() <= 2) return Double.MaxValue
    FastDTW.compare(leftTimeSeries, rightTimeSeries, new EuclideanDistance).getDistance
  }

}