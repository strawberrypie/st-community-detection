import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
  * Provides low level louvain community detection algorithm functions.  Generally used by LouvainHarness
  * to coordinate the correct execution of the algorithm though its several stages.
  *
  * For details on the sequential algorithm see:  Fast unfolding of communities in large networks, Blondel 2008
  */
object Louvain {
  private type Message = Map[Community, Double]
  type ComponentID = Long

  val defaultComponentId: PartitionID = -1

  def detectCommunities[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[ComponentID, ED] = {
    implicit val context = SparkContext.getOrCreate()

    val minProgress: Int = 1 // TODO accept parameters in a different way
    val progressCounter: Int = 1

    val weightedGraph = graph.mapEdges(_ => 1.0) // TODO check if graph is weighted

    val initialLouvainGraph = createLouvainGraph[VD](weightedGraph)
    val louvainGraph =
      louvain(initialLouvainGraph, minProgress, progressCounter)._2

    graph.outerJoinVertices(louvainGraph.vertices) {
      case (_, _, newData) =>
        newData.map(_.community.id).getOrElse(defaultComponentId)
    }
  }

  /**
    * Generates a new graph of type Graph[VertexState, Long] based on an input graph of type Graph[VD,Long].
    * Create the initial Louvain graph: weights are exchanged and summed in vertices.
    * The resulting graph can be used for louvain computation.
    *
    */
  private def createLouvainGraph[VD: ClassTag](
                                                graph: Graph[VD, Double]): Graph[VertexState, Double] = {
    val weightedGraph = graph.aggregateMessages[Double](triplet => {
      triplet.sendToDst(triplet.attr)
      triplet.sendToSrc(triplet.attr)
    }, _ + _)

    graph
      .outerJoinVertices(weightedGraph)((vid, _, weightOpt) => {
        val weight = weightOpt.getOrElse(0.0)
        VertexState(
          community = Community(vid, weight),
          nodeWeight = weight
        )
      })
      .partitionBy(PartitionStrategy.EdgePartition2D)
      .groupEdges(_ + _)
  }

  /**
    * For a graph of type Graph[VertexState,Long] label each vertex with a community to maximize global modularity.
    * (without compressing the graph)
    */
  private def louvain(graph: Graph[VertexState, Double],
                      minimalMovedVerticesThreshold: Int = 1,
                      maxProgressStoppedCount: Int = 1)(
                       implicit sc: SparkContext): (Double, Graph[VertexState, Double], Int) = {
    var louvainGraph = graph.cache()
    val graphWeight: Double = louvainGraph.vertices.values
      .map(state => state.internalWeight + state.nodeWeight)
      .reduce(_ + _)
    val totalGraphWeight = sc.broadcast(graphWeight)
    println("totalGraphWeight: " + totalGraphWeight.value)

    // gather community information from each vertex's local neighborhood
    var msgRDD: VertexRDD[Message] =
      louvainGraph.aggregateMessages(sendMsg, mergeMsg)
    msgRDD.count() // materializes the msgRDD and caches it in memory

    var movedVerticesCount = 0L - minimalMovedVerticesThreshold
    var even = false
    var iterationsCount = 0
    val maxIterationsCount = 100000
    var progressStoppedCount = 0
    var previousMovedVerticesCount = 0L
    do {
      iterationsCount += 1
      even = !even
      println(s"Iteration $iterationsCount")

      // label each vertex with its best community based on neighboring community information
      val labeledVertices =
        louvainVerticesJoin(louvainGraph, msgRDD, totalGraphWeight, even)
          .cache()

      // calculate new sigma total value for each community (total weight of each community)
      val communityUpdate = labeledVertices
        .mapValues { state =>
          (state.community.id, state.internalWeight + state.nodeWeight)
        }
        .values
        .reduceByKey(_ + _)
        .cache()

      // map each vertex ID to its updated community information
      val communityMapping = labeledVertices
        .map { case (vid, state) => (state.community.id, vid) }
        .join(communityUpdate)
        .map {
          case (communityId, (vid, communityWeight)) =>
            (vid, (communityId, communityWeight))
        }
        .cache()

      // join the community labeled vertices with the updated community info
      val updatedVertices = labeledVertices
        .join(communityMapping)
        .mapValues {
          case (state, (community, communityWeight)) =>
            state.copy(community = Community(community, communityWeight))
        }
        .cache()

      updatedVertices.count()
      labeledVertices.unpersist(blocking = false)
      communityUpdate.unpersist(blocking = false)
      communityMapping.unpersist(blocking = false)

      louvainGraph =
        louvainGraph.outerJoinVertices(updatedVertices)((_, old, state) =>
          state.getOrElse(old))
      louvainGraph.cache()

      // gather community information from each vertex's local neighborhood
      msgRDD = louvainGraph.aggregateMessages(sendMsg, mergeMsg).cache()
      msgRDD.count() // materializes the graph by forcing computation

      updatedVertices.unpersist(blocking = false)

      // half of the communities can switch on even cycles
      // and the other half on odd cycles (to prevent deadlocks)
      // so we only want to look for progress on odd cycles (after all vertices have had a chance to move)
      if (even) movedVerticesCount = 0
      movedVerticesCount = movedVerticesCount + louvainGraph.vertices
        .filter(_._2.changed)
        .count
      if (!even) {
        println(s"  # vertices moved: $movedVerticesCount")
        if (movedVerticesCount >= previousMovedVerticesCount - minimalMovedVerticesThreshold)
          progressStoppedCount += 1
        previousMovedVerticesCount = movedVerticesCount
      }
    } while (progressStoppedCount <= maxProgressStoppedCount &&
      (even || (movedVerticesCount > 0 && iterationsCount < maxIterationsCount)))
    println(s"\nCompleted in $iterationsCount cycles")

    // Use each vertex's neighboring community data to calculate the global modularity of the graph
    val newVertices =
      louvainGraph.vertices.innerJoin(msgRDD)((_, state, msgs) => {
        val k_i_in: Double = state.internalWeight + msgs
          .filter {
            case (Community(communityId, _), _) =>
              state.community.id == communityId
          }
          .values
          .sum
        val M = totalGraphWeight.value
        val k_i = state.nodeWeight + state.internalWeight
        val q = (k_i_in.toDouble / M) - ((state.community.weight * k_i) / math
          .pow(M, 2))
        if (q < 0) 0 else q
      })
    val actualQ = newVertices.values.reduce(_ + _)

    // return the modularity value of the graph along with the
    // graph. vertices are labeled with their community
    (actualQ, louvainGraph, iterationsCount / 2)

  }

  /**
    * Creates the messages passed between each vertex to convey neighborhood community data.
    */
  private def sendMsg(triplet: EdgeContext[VertexState, Double, Message]) = {
    triplet.sendToDst(Map(triplet.srcAttr.community -> triplet.attr))
    triplet.sendToSrc(Map(triplet.dstAttr.community -> triplet.attr))
  }

  /**
    * Merge neighborhood community data into a single message for each vertex
    */
  private def mergeMsg(m1: Message, m2: Message): Message = {
    (m1.keySet ++ m2.keySet)
      .map(vid => vid -> (m1.get(vid) ++ m2.get(vid)).sum)
      .toMap
  }

  /**
    * Join vertices with community data form their neighborhood and select the best community for each vertex to maximize change in modularity.
    * Returns a new set of vertices with the updated vertex state.
    */
  private def louvainVerticesJoin(louvainGraph: Graph[VertexState, Double],
                                  msgRDD: VertexRDD[Message],
                                  totalGraphWeight: Broadcast[Double],
                                  even: Boolean) = {
    louvainGraph.vertices.innerJoin(msgRDD)((vid, state, messages) => {
      val Community(bestCommunityId, bestCommunityWeight) =
        getBestCommunity(vid, state, messages, totalGraphWeight.value)

      // only allow changes from low to high communities on even cycles and high to low on odd cycles
      if (state.community.id != bestCommunityId &&
        ((even && state.community.id > bestCommunityId) || (!even && state.community.id < bestCommunityId))) {
        //        println(s"  $vid SWITCHED from ${state.id} to $bestCommunityId")
        state.copy(
          community = Community(bestCommunityId, bestCommunityWeight),
          changed = true
        )
      } else {
        state.copy(changed = false)
      }
    })
  }

  private def getBestCommunity(vertexId: VertexId,
                               state: VertexState,
                               messages: Message,
                               totalGraphWeight: Double): Community = {
    var bestCommunity = state.community.copy(weight = 0L)
    var maxDeltaQ = BigDecimal(0.0)
    //    println(s"Messages from vertexId $vertexId:")
    for {(community, communityEdgeWeight) <- messages} {
      val deltaQ =
        getDeltaQ(state, community, communityEdgeWeight, totalGraphWeight)
      //      println(s"   community: $id sigma:$communityWeight edgeweight:$communityEdgeWeight  q:$deltaQ")
      if (deltaQ > maxDeltaQ || (0 < deltaQ && deltaQ == maxDeltaQ && community.id > bestCommunity.id)) {
        maxDeltaQ = deltaQ
        bestCommunity = community
      }
    }
    bestCommunity
  }

  /**
    * Returns the change in modularity that would result from a vertex moving to a specified community.
    */
  private def getDeltaQ(state: VertexState,
                        targetCommunity: Community,
                        edgeWeight: Double,
                        totalGraphWeight: Double) = {
    val isCurrentCommunity = state.community.id == targetCommunity.id
    val M = BigDecimal(totalGraphWeight)
    val k_i_in_L =
      if (isCurrentCommunity) edgeWeight + state.internalWeight else edgeWeight
    val k_i_in = BigDecimal(k_i_in_L)
    val k_i = BigDecimal(state.nodeWeight + state.internalWeight)
    val sigma_tot =
      if (isCurrentCommunity) BigDecimal(targetCommunity.weight) - k_i
      else BigDecimal(targetCommunity.weight)
    val deltaQ = if (!(isCurrentCommunity && sigma_tot == 0.0)) {
      k_i_in - (k_i * sigma_tot / M)
    } else BigDecimal(0.0)
    deltaQ
  }

  def detectCommunitiesWeighted[VD: ClassTag](
                                               weightedGraph: Graph[VD, Double]): Graph[ComponentID, Double] = {
    implicit val context = SparkContext.getOrCreate()

    val minProgress: Int = 1 // TODO accept parameters in a different way
    val progressCounter: Int = 1

    val initialLouvainGraph = createLouvainGraph(weightedGraph)
    val louvainGraph =
      louvain(initialLouvainGraph, minProgress, progressCounter)._2

    weightedGraph.outerJoinVertices(louvainGraph.vertices) {
      case (_, _, newData) =>
        newData.map(_.community.id).getOrElse(defaultComponentId)
    }
  }

  /**
    * Compress a graph by its communities, aggregate both internal node weights and edge
    * weights within communities.
    */
  private def contractGraph(
                             graph: Graph[VertexState, Long],
                             debug: Boolean = true): Graph[VertexState, Long] = {
    // aggregate the edge weights of self loops. edges with both src and dst in the same community.
    // WARNING  can not use graph.mapReduceTriplets because we are mapping to new vertexIds
    val internalEdgeWeights = graph.triplets
      .flatMap(
        et =>
          if (et.srcAttr.community.id == et.dstAttr.community.id)
            Some((et.srcAttr.community.id, 2 * et.attr))
          else None) // count the weight from both nodes  // count the weight from both nodes
      .reduceByKey(_ + _)

    // aggregate the internal weights of all nodes in each community
    val internalWeights = graph.vertices.values
      .map(vdata => (vdata.community.id, vdata.internalWeight))
      .reduceByKey(_ + _)

    // join internal weights and self edges to find new interal weight of each community
    val newVertices = internalWeights
      .leftOuterJoin(internalEdgeWeights)
      .map {
        case (vid, (weight1, weight2Option)) =>
          val weight2 = weight2Option.getOrElse(0L)
          (vid,
            VertexState(community = Community(vid),
              internalWeight = weight1 + weight2))
      }
      .cache()

    // translate each vertex edge to a community edge
    val edges = graph.triplets
      .flatMap(et => {
        val src = math.min(et.srcAttr.community.id, et.dstAttr.community.id)
        val dst = math.max(et.srcAttr.community.id, et.dstAttr.community.id)
        if (src != dst) Some(new Edge(src, dst, et.attr))
        else None
      })
      .cache()

    // generate a new graph where each community of the previous
    // graph is now represented as a single vertex
    val contractedGraph = Graph(newVertices, edges)
      .partitionBy(PartitionStrategy.EdgePartition2D)
      .groupEdges(_ + _)

    // calculate the weighted degree of each node
    val nodeWeights = contractedGraph.aggregateMessages[Long](edge => {
      edge.sendToSrc(edge.attr)
      edge.sendToDst(edge.attr)
    }, _ + _)

    // fill in the weighted degree of each node
    // val louvainGraph = contractedGraph.joinVertices(nodeWeights)((vid,data,weight)=> {
    val louvainGraph = contractedGraph
      .outerJoinVertices(nodeWeights)((_, data, weightOption) => {
        val weight = weightOption.getOrElse(0L)
        data.copy(
          nodeWeight = weight,
          community =
            data.community.copy(weight = weight + data.internalWeight)
        )
      })
      .cache()
    louvainGraph.vertices.count()
    louvainGraph.triplets.count() // materialize the graph

    newVertices.unpersist(blocking = false)
    edges.unpersist(blocking = false)
    louvainGraph
  }

  private case class Community(id: Long = -1, weight: Double = 0)
    extends Serializable

  private case class VertexState(community: Community = Community(),
                                 internalWeight: Double = 0.0, // self edges
                                 nodeWeight: Double = 0.0, //out degree
                                 changed: Boolean = false)
    extends Serializable

  /*
    // debug printing
    private def printLouvain(graph:Graph[VertexState, Long]) = {
      print("\ncommunity label snapshot\n(vid,community,sigmaTot)\n")
      graph.vertices.mapValues((vid,vdata)=> (vdata.community,vdata.communitySigmaTot)).collect().foreach(f=>println(" "+f))
    }


    // debug printing
    private def printEdgeTriplets(graph:Graph[VertexState, Long]) = {
      print("\ncommunity label snapshot FROM TRIPLETS\n(vid,community,sigmaTot)\n")
      graph.triplets
        .flatMap(e => Iterator((e.srcId, e.srcAttr.community, e.srcAttr.communitySigmaTot),
                               (e.dstId, e.dstAttr.community, e.dstAttr.communitySigmaTot)))
        .collect()
        .foreach(edge => println(s" $edge"))
    }
 */
}
