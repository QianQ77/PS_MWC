package mwcp.spark

import mwcp.spark.MWCP.Subproblem
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}

import scala.collection.Map
import scala.collection.mutable.Buffer

/**
  * Created by qiuqian on 4/28/18.
  */
object Partition2 extends Watcher with LogHelper{

  var global_max_weight: Double = 0

  var zk: ZooKeeper = null

  def partition(weightedGraph: Graph[Double, Int], zkServerIp: String) : VertexRDD[(Double, Set[VertexId])]= {

    if(zk == null) {
      zk = ZKHelper.startZK(this, zkServerIp)
      global_max_weight = ZKHelper.readWeightFromZookeeper(zk, this)
    }

    weightedGraph.cache()

    var ubDegreeGraph = ReduceGraph.getUBDegreeGraph(weightedGraph).cache()
    logInfo("ubDegreeGraph generated")

    //cost 20.8s
    //The output RDD's attribute: (upperBoundHNb, set of Higher order neighbors' id)
    //upperBoundHNb = sum of weights of Higher oder neighbors + this vertex's weight
    val hnbVertices = ubDegreeGraph.aggregateMessages[(Buffer[VertexId], Double)](
      triplet => {
        //srcAttr and dstAttr: (weight, upperbound, degree)
        //send message only if the upperbounds of src and desc are both larger than global_max_weight
        if(triplet.srcAttr._2 > global_max_weight && triplet.dstAttr._2 > global_max_weight) {
          if (triplet.srcAttr._3 > triplet.dstAttr._3) {
            //if src has higher degree, send src's id and weight to dest
            triplet.sendToDst((Buffer(triplet.srcId), triplet.srcAttr._1))
          } else if (triplet.srcAttr._3 == triplet.dstAttr._3) {
            // if src and dst have equal degree, send higher id and weight to lower id vertex
            if (triplet.srcId > triplet.dstId) triplet.sendToDst((Buffer(triplet.srcId), triplet.srcAttr._1))
            else triplet.sendToSrc((Buffer(triplet.dstId), triplet.dstAttr._1))
          } else {
            // if desc has higher degree, send desc's id and weight to src
            triplet.sendToSrc((Buffer(triplet.dstId), triplet.dstAttr._1))
          }
        }

      },
      //merge neighbors and add up the weights
      (n1, n2) => (n1._1 ++ n2._1, n1._2 + n2._2)
    ).leftJoin(weightedGraph.vertices)((vid, setAndSumHNb, weight) =>
      (weight.getOrElse(0D) + setAndSumHNb._2, setAndSumHNb._1.toSet)).
      filter(vertex => vertex._2._1 > global_max_weight) //if sum of this vertex'weight and its higher-ordering neighbors' weight is not greater than current
    // clique weight, we don't need to process the subgraph which is generated by this vertex

    weightedGraph.unpersist()
    ubDegreeGraph.unpersist()
    hnbVertices
  }

  override def process(watchedEvent: WatchedEvent) = {
    global_max_weight = ZKHelper.readWeightFromZookeeper(zk, this)
  }

}
