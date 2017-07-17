import org.apache.spark.graphx.{EdgeTriplet, Graph, Pregel, VertexId}

import scala.collection.mutable
import scala.reflect.ClassTag
//import scala.reflect.ClassTag

object OverlappingCommunityDetection {

  /**
    * Run Overlapping Community Detection for detecting overlapping communities in networks.
    *
    * OLPA is an overlapping community detection algorithm. It is based on standard Label propagation
    * but instead of single community per node, multiple communities can be assigned per node.
    *
    * @tparam ED the edge attribute type (not used in the computation)
    * @param graph           the graph for which to compute the community affiliation
    * @param maxSteps        the number of supersteps of OLPA to be performed. Because this is a static
    *                        implementation, the algorithm will run for exactly this many supersteps.
    * @param communitiesCount 	the maximum number of communities to be assigned to each vertex
    * @return a graph with list of vertex attributes containing the labels of communities affiliation
    */

  def run[VD, ED : ClassTag](graph: Graph[VD, ED], maxSteps: Int, communitiesCount: Int): Graph[Iterable[VertexId], ED] = {
    require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got $maxSteps")
    require(communitiesCount > 0, s"Number of communities must be greater than 0, but got $communitiesCount")

    type Counts = mutable.Map[VertexId, Double]

    val threshold: Double = 1.0 / communitiesCount

    val lpaGraph: Graph[Counts, ED] = graph.mapVertices {
      case (vid, _) => mutable.HashMap[VertexId, Double](vid -> 1)
    }

    def sendMessage(e: EdgeTriplet[Counts, ED]) = {
      Iterator((e.srcId, e.dstAttr), (e.dstId, e.srcAttr))
    }

    def mergeMessage(count1: Counts, count2: Counts): Counts = {
      val result = new mutable.HashMap[VertexId, Double]()
      (count1.keySet ++ count2.keySet)
        .map(vid => vid -> (count1.get(vid) ++ count2.get(vid)).sum)
        .foreach(result += _)
      result
    }

    def vertexProgram(vid: VertexId, attr: Counts, message: Counts): Counts = {
      if (message.isEmpty) {
        attr
      } else {
        val coefficientSum = message.values.sum

        //Normalize the map so that every node has total coefficientSum as 1
        val normalizedMap = message.mapValues(_ / coefficientSum)


        val resMap: Counts = new mutable.HashMap[VertexId, Double]
        var maxRow: VertexId = 0L
        var maxRowValue: Double = Double.MinValue

        normalizedMap.foreach{ case (vertexId, ratio) =>
          if (ratio >= threshold) {
            resMap += (vertexId -> ratio)
          } else if (ratio > maxRowValue) {
            maxRow = vertexId
            maxRowValue = ratio
          }
        }

        // Add maximum value node in result map if there is no node with sum greater then threshold
        if (resMap.isEmpty) {
          resMap += (maxRow -> maxRowValue)
        }

        val updatedCoefficientsSum = resMap.values.sum
        resMap.map { case (key, ratio) =>
          key -> (ratio / updatedCoefficientsSum)
        }
      }
    }

    val initialMessage = mutable.Map[VertexId, Double]()

    val overlapCommunitiesGraph = Pregel(lpaGraph, initialMessage, maxIterations = maxSteps)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)

    overlapCommunitiesGraph.mapVertices((vertexId, vertexProperties) => vertexProperties.keys)
  }
}