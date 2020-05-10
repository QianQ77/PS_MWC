package mwcp.spark

import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

/**
  * Created by qiuqian on 3/21/18.
  */
object ReduceGraphOld extends LogHelper{

  /**
    *
    * @param graph vertex weighted graph G = (Vertices, Edges)
    * @param neighboursRDD
    * @param optimumWeight an optimum clique weight value
    * @return a simplified graph of G
    */
  def reduceGraph(graph: Graph[Double, Int], neighboursRDD: VertexRDD[Array[(VertexId, Double)]], optimumWeight: Double) : Graph[Double, Int] = {

    graph.cache()

    val startTime = System.currentTimeMillis()
    neighboursRDD.cache()

    val nodesCount = graph.numVertices

    logInfo("Reducing Graph " + graph + ", optimumWeight: " + optimumWeight + "...")
    logInfo("Graph " + graph + " size before reducing: # of nodes = " + graph.vertices.count() + " # of edges = " + graph.edges.count())


    //upboundRDD contains (VertexId, (upboundWeight, weight))
    var upboundRDD:RDD[(VertexId, (Double, Double))] = neighboursRDD.mapValues{
       value =>
         if(value.size == 0) 0
         else value.map{case (vid, weight) => weight}.reduce(_ + _) //get the sum of neighbors' weights for each vertex
    }.join(graph.vertices).mapValues{case (neighborsWeight, weight) => (neighborsWeight + weight, weight)} //get the upper bound weight of MWCL contains each vertex

    upboundRDD.cache()

    //toBeRemovedArray contains (VertexId,  weight)
    var toBeRemovedArray:Array[(VertexId, Double)] = upboundRDD.filter(_._2._1 <= optimumWeight). //if upperbound <= optimumWeight, we should remove the vertex
      mapValues{case (upbound, weight) => weight}.collect()

    var ifContinue = true

    //iteratively reduce the graph
    while(ifContinue && nodesCount < 1000){
      // for each vertex, get sum of weights of its neighbors in toBeRemovedArray
      var toBeRemovedNeighboursWeightRDD = neighboursRDD.mapValues{neighbours => neighbours.intersect(toBeRemovedArray)}.filter(_._2.length > 0).
        mapValues{arrays => arrays.map{case (vid, weight) => weight}.reduce(_ + _)}

      toBeRemovedNeighboursWeightRDD.cache()

      var newToBeRemovedArray:Array[(VertexId, Double)] = upboundRDD.leftOuterJoin(toBeRemovedNeighboursWeightRDD).
        mapValues{case ((upboud, weight), Some(removingWeights)) => (upboud - removingWeights, weight)
        case ((upboud, weight), None) => (upboud, weight)}.filter(_._2._1 <= optimumWeight). //if upperbound <= optimumWeight, we should remove the vertex
        mapValues{case (upbound, weight) => weight}.collect()

      if(newToBeRemovedArray.length == toBeRemovedArray.length) {
        ifContinue = false
      }else{
        toBeRemovedArray = newToBeRemovedArray
      }
    }
    //remove toBeRemoved vertices and relative edges from original graph
    var resultGraph = graph.subgraph(vpred = (vid, weight) => !toBeRemovedArray.contains((vid, weight)))
    logInfo("Graph " + graph + " size after reducing: # of nodes = " + resultGraph.vertices.count() + " # of edges = " + resultGraph.edges.count())
    val timeCost = (System.currentTimeMillis() - startTime) / 1000
    logInfo("Time Cost: " + timeCost + " seconds")

    upboundRDD.unpersist()
    graph.unpersist()

    resultGraph
  }

}
