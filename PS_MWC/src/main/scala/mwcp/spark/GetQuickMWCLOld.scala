package mwcp.spark

import mwcp.spark.MWCP.MWCL
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId, VertexRDD}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by qiuqian on 3/21/18.
  */
object GetQuickMWCLOld extends LogHelper{

  // get a mwcl value within 3 second, this value is used for first-run graph reduction and pruning
  // while run on bio-dmela.mtx, 3 seconds get result: MWCL(List(7393, 112),307.0); 5, 8 seconds get same result;

  // get a mwcl: List(1069126) 127.0 in 104 seconds for ca-hollywood-2009.mtx

  def getQuickMwcl(graph: Graph[Double, Int]): MWCL = {
    val startTime = System.currentTimeMillis()

    var curr_vertices = ArrayBuffer[VertexId]()
    var curr_weight: Double = 0
    var workingGraph: Graph[Double, Int] = graph
    var max_weight_vertex: (VertexId, Double) = workingGraph.vertices.max

    curr_vertices.append(max_weight_vertex._1)
    curr_weight += max_weight_vertex._2

    var nbRDD: VertexRDD[Array[VertexId]] = workingGraph.collectNeighborIds(EdgeDirection.Either)
    nbRDD.cache()

    //initially, candidates is an Array which contains all neighbors of working vertex
    var candidates: Array[VertexId] = nbRDD.lookup(max_weight_vertex._1)(0)

    while(!candidates.isEmpty && System.currentTimeMillis() - startTime < 3000) {
      workingGraph = workingGraph.subgraph(vpred = (vid, weight) => candidates contains vid)

      nbRDD.unpersist()
      nbRDD = workingGraph.collectNeighborIds(EdgeDirection.Either)
      nbRDD.cache()

      max_weight_vertex = workingGraph.vertices.max

      curr_vertices.append(max_weight_vertex._1)
      curr_weight += max_weight_vertex._2

      //new candidates should be old candidates intersect neighbors of working vertex
      candidates = candidates.intersect(nbRDD.lookup(max_weight_vertex._1)(0))
    }
    val costTime = (System.currentTimeMillis() - startTime) / 1000
    logInfo("Quickly get a mwcl: " + curr_vertices.toList + " " + curr_weight + " in " + costTime + " seconds" )

    MWCL(curr_vertices.toList, curr_weight)

  }

}
