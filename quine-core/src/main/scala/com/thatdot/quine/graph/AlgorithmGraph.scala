package com.thatdot.quine.graph

import scala.concurrent.Future

import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.{ByteString, Timeout}

import com.thatdot.quine.graph.cypher.{CompiledQuery, Location}
import com.thatdot.quine.graph.messaging.AlgorithmMessage.{GetRandomWalk, RandomWalkResult}
import com.thatdot.quine.graph.messaging.SpaceTimeQuineId
import com.thatdot.quine.model.QuineIdHelpers._
import com.thatdot.quine.model.{Milliseconds, QuineId}

trait AlgorithmGraph extends BaseGraph {

  private[this] def requireCompatibleNodeType(): Unit =
    requireBehavior[AlgorithmGraph, behavior.AlgorithmBehavior]

  object algorithms {

    /** Generate a random walk through the graph starting at the supplied node.
      *
      * @param startingNode Node at which to begin the walk. This ID will be the first in the returned list.
      * @param collectQuery A constrained OnNode Cypher query to fetch results to fold into the random walk results
      * @param length       The max number of hops to take from the starting node. The returned list may be shorter
      *                     than this length if the graph structure limits the options.
      * @param returnParam  the `p` parameter for biasing random walks back to the previous node.
      * @param inOutParam   the `q` parameter for biasing random walks toward BFS or DFS.
      * @param walkSeqNum   A sequence number to track and distinguish multiple walks made from the same starting node.
      *                     This is needed to deterministically reproduce the same walk by providing a random seed. If
      *                     defined, this sequence number gets prepended to the random seed string.
      * @param seedOpt      Optionally specify the random seed used for this walk. The total random seed becomes a
      *                     hashcode of the combination of: starting node, walk sequence number, and this string.
      * @param atTime       Use nodes from this time period or the thoroughgoing present.
      *
      * @return A list of QuineIDs representing the walk taken from the starting node. The starting node
      *         ID is the first QuineID in the returned list. The length of this list will vary based on
      *         the graph structure, but will be no more than 1+length.
      *
      * This implementation is based on the Node2Vec procedure for random walk generation, which uses two parameters,
      * "p" (the "return" parameter) and "q" (the "in-out" parameter).
      * From the paper - https://cs.stanford.edu/~jure/pubs/node2vec-kdd16.pdf :
      *
      * *Return parameter*, p. Parameter p controls the likelihood of immediately revisiting a node in the walk.
      * Setting it to a high value (> max(q, 1)) ensures that we are less likely to sample an already visited node
      * in the following two steps (unless the next node in the walk had no other neighbor). This strategy encourages
      * moderate exploration and avoids 2-hop redundancy in sampling. On the other hand, if p is low (< min(q, 1)),
      * it would lead the walk to backtrack a step (Figure 2) and this would keep the walk "local" close to the
      * starting node u.
      *
      * *In-out parameter*, q. Parameter q allows the search to differentiate between "inward" and "outward" nodes.
      * Going back to Figure 2, if q > 1, the random walk is biased towards nodes close to node t. Such walks obtain
      * a local view of the underlying graph with respect to the start node in the walk and approximate BFS behavior
      * in the sense that our samples comprise of nodes within a small locality.
      * In contrast, if q < 1, the walk is more inclined to visit nodes which are further away from the node t.
      * Such behavior is reflective of DFS which encourages outward exploration. However, an essential difference
      * here is that we achieve DFS-like exploration within the random walk framework. Hence, the sampled nodes are
      * not at strictly increasing distances from a given source node u, but in turn, we benefit from tractable
      * preprocessing and superior sampling efficiency of random walks. Note that by setting πv,x to be a function
      * of the preceding node in the walk t, the random walks are 2nd order Markovian.
      */
    def randomWalk(
      startingNode: QuineId,
      collectQuery: CompiledQuery[Location.OnNode],
      length: Int,
      returnParam: Double,
      inOutParam: Double,
      walkSeqNum: Option[Int], // If none, you can manually prepend an integer to `seedOpt` to generate the same seed
      seedOpt: Option[String],
      namespace: NamespaceId,
      atTime: Option[Milliseconds],
    )(implicit
      timeout: Timeout,
    ): Future[RandomWalkResult] = relayAsk(
      SpaceTimeQuineId(startingNode, namespace, atTime),
      GetRandomWalk(
        collectQuery,
        length,
        returnParam,
        inOutParam,
        walkSeqNum.fold(seedOpt)(num => seedOpt.map(s => s"$num$s")),
        _,
      ),
    )

    /** @param saveSink      An output sink which will handle receiving and writing each of the final walk results.
      * @param collectQuery  A constrained OnNode Cypher query to fetch results to fold into the random walk results
      * @param length        Number of steps to take through the graph. Max walk size is `length + 1`, which includes
      *                      the origin. The size will not always be this large, since it depends on the graph shape
      *                      and parameters.
      * @param walksPerNode  The count of walks which will be generated from each starting node, each random.
      * @param returnParam   the `p` parameter for biasing random walks back to the previous node.
      * @param inOutParam    the `q` parameter for biasing random walks toward BFS or DFS.
      * @param randomSeedOpt An optional arbitrary string to consistently set the random seed.
      * @param atTime        Time at which to query nodes. None queries the thoroughgoing present.
      * @param parallelism   Number of simultaneous walks to evaluate. This only the runtime, not the results.
      *
      * @return              The materialized result of running the saveSink. This will be a future representing the
      *                      completion of the file write operation.
      */
    def saveRandomWalks[SinkMat](
      saveSink: Sink[ByteString, Future[SinkMat]],
      collectQuery: CompiledQuery[Location.OnNode],
      length: Int,
      walksPerNode: Int,
      returnParam: Double,
      inOutParam: Double,
      randomSeedOpt: Option[String] = None,
      namespace: NamespaceId,
      atTime: Option[Milliseconds] = None,
      parallelism: Int = 16,
    )(implicit
      timeout: Timeout,
    ): Future[SinkMat] = {
      requireCompatibleNodeType()
      enumerateAllNodeIds(namespace)
        .flatMapConcat(qid => Source(0 until walksPerNode).map(qid -> _))
        .mapAsync(parallelism) { case (qid, i) =>
          randomWalk(qid, collectQuery, length, returnParam, inOutParam, Some(i), randomSeedOpt, namespace, atTime)
            .map(walk =>
              // Prepending the QuineId as the first row value in the final output to indicate where each walk began.
              // Note that if a user provides a query, it could be that the node ID never shows up; this mitigates that.
              ByteString(s"${(qid.pretty(idProvider) :: walk.acc).mkString(",")}\n"),
            )(nodeDispatcherEC)
        }
        .runWith(saveSink)
    }

  }
}

object AlgorithmGraph {
  object defaults {

    /* WARNING: the `walkPrefix` and `walkSuffix` values are duplicated in AlgorithmRoutes which is not available here.
     * Beware of changes in one place not mirrored to the other!
     */
    val walkPrefix = "MATCH (thisNode) WHERE id(thisNode) = $n "
    val walkSuffix = "RETURN id(thisNode)"

    val walkQuery: String = walkPrefix + walkSuffix
    val walkLength = 10
    val walkCount = 5
    val inOutParam = 1d
    val returnParam = 1d
  }

  /** Check if a graph supports algorithm operations and refine it if possible */
  @throws[IllegalArgumentException]("if the graph does not implement AlgorithmGraph")
  def getOrThrow(context: => String, graph: BaseGraph): AlgorithmGraph =
    graph match {
      case g: AlgorithmGraph => g
      case _ => throw new IllegalArgumentException(s"$context requires a graph that implements Algorithms")
    }
}
