import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy}
import org.apache.spark.SparkContext
import com.github.nscala_time.time.RichString._
import org.apache.spark.rdd.RDD

import scala.tools.nsc.io.File

object Main extends App {

  def getCheckinsDataset(filename: String)(implicit sc: SparkContext): RDD[(Long, Iterable[Checkin])] =
    sc.textFile(filename)
      .map(token => token.split('\t'))
      .collect(Checkin.fromArray)
      .map(checkin => checkin.userId.toLong -> checkin)
      .groupByKey

  def getEdges(filename: String)(implicit sc: SparkContext): RDD[Edge[Int]] =
    sc.textFile(filename)
      .map(_.split('\t').map(_.toInt))
      .collect { case Array(from, to) => Edge[Int](from, to, -1) }

  implicit val sc: SparkContext = SparkContext.getOrCreate()

  val checkins = getCheckinsDataset("/user/akiselev/loc-brightkite_totalCheckins.txt").cache()
  val edges = getEdges("/user/akiselev/loc-brightkite_edges.txt").cache()

  val partitionsCount: Int = 36

  val graph = Graph[Iterable[Checkin], Int](checkins, edges)
    .partitionBy(PartitionStrategy.EdgePartition2D, partitionsCount)
    .cache()
  checkins.unpersist(blocking = false)
  edges.unpersist(blocking = false)

  val weightedGraph = graph
    .mapTriplets(triplet => 1 / (1 + CheckinTimeSeries.metric(triplet.srcAttr, triplet.dstAttr)))
    .partitionBy(PartitionStrategy.EdgePartition2D, partitionsCount)
    .cache()
  val communitiesGraph = Louvain.detectCommunitiesWeighted(weightedGraph)
    .partitionBy(PartitionStrategy.EdgePartition2D, partitionsCount)
    .cache()
  weightedGraph.unpersist(blocking = false)

  communitiesGraph.vertices
    .map { case (vertexId, communityId) => vertexId.toString + " " + communityId.toString }
    .saveAsTextFile("/user/akiselev/brightkite_communities.txt")

}
