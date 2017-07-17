import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.SparkContext
import org.apache.spark.sql._

import com.github.nscala_time.time.RichString._
import org.apache.spark.rdd.RDD


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

  implicit val session: SparkSession = SparkSession.builder()
    .appName("ST-detection")
    .getOrCreate()
  implicit val sc: SparkContext = session.sparkContext
//  import session.sqlContext.implicits._
  import session.implicits._

  val checkins = getCheckinsDataset("/user/akiselev/loc-brightkite_totalCheckins.txt")
  val edges = getEdges("/user/akiselev/loc-brightkite_edges.txt")

  val graph = Graph[Iterable[Checkin], Int](checkins, edges)

  val weightedGraph = graph.mapTriplets(triplet => 1 / (1 + CheckinTimeSeries.metric(triplet.srcAttr, triplet.dstAttr)))
  val communitiesGraph = Louvain.detectCommunities(weightedGraph)

  communitiesGraph.vertices.toDS.repartition(1)
    .write
      .option("header", "true")
    .csv("/user/akiselev/brightkite_communities.csv")

}
