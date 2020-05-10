package mwcp.spark

import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}

import scala.collection.mutable.Buffer

/**
  * Created by qiuqian on 3/24/18.
  */
object ReduceGraph extends LogHelper{

  //Vertices' property: (set of neighbors id, sum of weights of neighbors)
  def getNbVertices(weightedGraph: Graph[Double, Int]): VertexRDD[(Buffer[VertexId], Double)] = {
    weightedGraph.aggregateMessages[(Buffer[VertexId], Double)] (
      triplet => {
        triplet.sendToDst((Buffer(triplet.srcId), triplet.srcAttr))
        triplet.sendToSrc((Buffer(triplet.dstId), triplet.dstAttr))

      },
      //merge neighbors and add up the weights
      (n1, n2) => (n1._1 ++ n2._1, n1._2 + n2._2)
    )
  }

  //Vertex attribute: (weight, upperbound, degree)
  def getUBDegreeGraph(workingGraph: Graph[Double, Int]) : Graph[(Double, Double, Int), Int] = {
    val nbVertices: VertexRDD[(Buffer[VertexId], Double)] = getNbVertices(workingGraph)

    var nbGraph: Graph[(Double, (Buffer[VertexId], Double)), Int] =
      workingGraph.outerJoinVertices(nbVertices)((vid, weight,optNb) =>
        (weight, optNb.getOrElse((Buffer(), 0))))

      //Vertex property: (weight, upperBound, degree)
      //upperBound = weight + sumOfNeighborsWeight
      nbGraph.mapVertices((vid, attr) => (attr._1, attr._1 + attr._2._2, attr._2._1.size))
  }

  def reduceGraph(weightedGraph: Graph[Double, Int],  optimumWeight: Double) : Graph[Double, Int] = {

    var workingGraph = weightedGraph

    workingGraph.cache()

    logInfo("Reducing Graph New")
    logInfo("Before Reducing, # of nodes: " +  workingGraph.numVertices +
      " # of edges: " + workingGraph.numEdges)

    val startTime = System.currentTimeMillis()



    //(set of neighbors id, sum of weights of neighbors)
    val nbVertices: VertexRDD[(Buffer[VertexId], Double)] = getNbVertices(workingGraph)
    //nbVertices.cache()

    workingGraph.unpersist()

    //vertex property: (weight, (neighbors, sumOfNeighborsWeight))
    var nbGraph: Graph[(Double, (Buffer[VertexId], Double)), Int] =
      workingGraph.outerJoinVertices(nbVertices)((vid, weight,optNb) =>
        (weight, optNb.getOrElse((Buffer(), 0))))

    //nbGraph.cache()

    //nbVertices.unpersist()

    var reducedGraph: Graph[Double, Int] = nbGraph.
    //Vertex property: (weight, upperBound)
    //upperBound = weight + sumOfNeighborsWeight
    mapVertices((vid, attr) => (attr._1, attr._1 + attr._2._2)).
      subgraph(vpred = (vid, attr) => attr._2 > optimumWeight).
      mapVertices((vid, attr) => attr._1)

    reducedGraph.cache()

    logInfo("After Reducing, # of nodes: " + reducedGraph.numVertices +
      " # of edges: " + reducedGraph.numEdges)
    logInfo("Time Cost: " + (System.currentTimeMillis() - startTime) + " ms")

    reducedGraph

  }

  def reduceGraphIter(weightedGraph: Graph[Double, Int],  optimumWeight: Double) : Graph[Double, Int] = {

    var workingGraph = weightedGraph

    var reducedGraph = reduceGraph(workingGraph, optimumWeight)
    //If in the previous round of reducing, the number of removed vertex is more than 20, then go on reducing
    while(reducedGraph.numVertices < workingGraph.numVertices - 20) {
      reducedGraph = reduceGraph(reducedGraph, optimumWeight)
      workingGraph = reducedGraph
    }

    reducedGraph
  }


}
