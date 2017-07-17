import com.fastdtw.timeseries.TimeSeries

class ScalaTimeSeries(items: Seq[ScalaTimeSeriesItem])
  extends TimeSeries
    with Serializable {
  assert(items.size > 1)

  override def getMeasurementVector(pointIndex: Int): Array[Double] = items(pointIndex).point.values

  override def numOfDimensions(): Int = items.head.point.dimensions

  override def getMeasurement(pointIndex: Int, valueIndex: Int): Double = items(pointIndex).point(valueIndex)

  override def size(): Int = items.size

  override def getTimeAtNthPoint(n: Int): Double = items(n).time
}


case class ScalaTimeSeriesItem(time: Double, point: ScalaTimeSeriesPoint)
  extends Serializable


case class ScalaTimeSeriesPoint(values: Array[Double])
  extends Serializable {

  def apply(dimension: Int): Double = values(dimension)
  def dimensions: Int = values.length
}