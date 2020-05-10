package mwcp.spark

import mwcp.spark.MWCP.MWCL
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId, VertexRDD}
import scala.collection.mutable.ArrayBuffer

/**
  * Created by qiuqian on 3/25/18.
  */
object GetQuickMWCL  extends LogHelper{

  def getQuickMwcl(graph: Graph[Double, Int]): MWCL = {
    val startTime = System.currentTimeMillis()

    var curr_vertices = ArrayBuffer[VertexId]()
    var curr_weight: Double = 0

    //take a sample graph from original graph
    //get a mwcl: List(1599) 200.0 in 3.1s for ca-hollywood-2009.mtx
    //var workingGraph: Graph[Double, Int] = graph.subgraph(vpred = (id, attr) => id <= 10000).cache

    //get a mwcl: List(199, 198) 399.0 in 3.2s for ca-hollywood-2009.mtx
    //with 3s time limit, get a mwcl with weight 1572.0 in 3.3 ms for web-uk-2005 on local machine
    //using this clique weight, reducedSubTotal: 129603, solvedSubProbemNumber: 29
    //total cost is 51s
    //without time limit, get a mwcl with weight 33242 in 31.9s for web-uk-2005 on local machine
    //using this clique weight, reducedSubTotal: 129614, solvedSubProbemNumber: 18
    //total cost is 65s
    var workingGraph: Graph[Double, Int] = graph.subgraph(vpred = (id, _) => id <= 500).cache

    //get a mwcl: List(199) 200.0 in 3s for ca-hollywood-2009.mtx
    //var workingGraph: Graph[Double, Int] = graph.subgraph(vpred = (id, attr) => id <= 500).cache

    // get a mwcl: List(101799) 200.0 in 5.7s for web-uk-2005
    // get a mwcl: MWCL(List(7393, 112),307.0) in 3s for bio-dmela.mtx; 5, 8 seconds get same result;
    // get a mwcl: List(602399) 200.0 in 68s for ca-hollywood-2009.mtx
    //var workingGraph: Graph[Double, Int] = graph.cache()

    var degreeGraph = workingGraph.outerJoinVertices(workingGraph.degrees)((vid, weight, degOpt) => (weight, degOpt.getOrElse(0)))

    degreeGraph.cache()

    //Order vertex by weight and then degree
    //Has better performance than orderingByDegreeAndWeight
    val orderingByWeightAndDegree = new Ordering[Tuple2[VertexId, Tuple2[Double, Int]]]() {
      override def compare(x: (VertexId, (Double, Int)), y: (VertexId, (Double, Int))): Int =
        if(x._2._1 > y._2._1 || x._2._1 < y._2._1) {
          Ordering[Double].compare(x._2._1, y._2._1)
        }else {
          Ordering[Int].compare(x._2._2, y._2._2)
        }
    }

    //Order vertex by degree and then weight
    val orderingByDegreeAndWeight = new Ordering[Tuple2[VertexId, Tuple2[Double, Int]]]() {
      override def compare(x: (VertexId, (Double, Int)), y: (VertexId, (Double, Int))): Int =
        if(x._2._2 > y._2._2 || x._2._2 < y._2._2) {
          Ordering[Int].compare(x._2._2, y._2._2)
        }else {
          Ordering[Double].compare(x._2._1, y._2._1)
        }
    }

    //choose the vertex with highest weight and degree
    var max_weight_degree_vertex: (VertexId, (Double, Int)) =
      degreeGraph.vertices.max()(orderingByWeightAndDegree)

      curr_vertices.append(max_weight_degree_vertex._1)
    curr_weight += max_weight_degree_vertex._2._1

    //initially, candidates is an Array which contains all neighbors of working vertex
    var candidates: Array[VertexId] = workingGraph.collectNeighborIds(EdgeDirection.Either).lookup(max_weight_degree_vertex._1)(0)

    //while(!candidates.isEmpty && System.currentTimeMillis() - startTime < 3000) {
    while(!candidates.isEmpty) {

      workingGraph = workingGraph.subgraph(vpred = (vid, weight) => candidates contains vid).cache()

      degreeGraph = workingGraph.outerJoinVertices(workingGraph.degrees)((vid, weight, degOpt) => (weight, degOpt.getOrElse(0))).cache()

      //choose the vertex with highest weight and degree
      max_weight_degree_vertex =
        degreeGraph.vertices.max()(orderingByWeightAndDegree)

      curr_vertices.append(max_weight_degree_vertex._1)
      curr_weight += max_weight_degree_vertex._2._1

      //new candidates should be old candidates intersect neighbors of working vertex
      candidates = candidates.intersect(workingGraph.collectNeighborIds(EdgeDirection.Either).lookup(max_weight_degree_vertex._1)(0))
    }
    val costTime = System.currentTimeMillis() - startTime
    logInfo("Quickly get a mwcl: " + curr_vertices.toList + " " + curr_weight + " in " + costTime + " ms" )

    MWCL(curr_vertices.toList, curr_weight)

  }

  def getQuickMwcl2(graph: Graph[Double, Int]): MWCL = {
    val startTime = System.currentTimeMillis()

    var curr_vertices = ArrayBuffer[VertexId]()
    var curr_weight: Double = 0
    var workingVertex = graph.vertices.sample(false, 0.01).collect()
    var workingGraph: Graph[Double, Int] = graph.subgraph(vpred = (vid, weight) => workingVertex.contains(vid))

    workingGraph.cache()
    var degreeGraph = workingGraph.outerJoinVertices(workingGraph.degrees)((vid, weight, degOpt) => (weight, degOpt.getOrElse(0)))

    degreeGraph.cache()

    //Order vertex by weight and then degree
    //Has better performance than orderingByDegreeAndWeight
    val orderingByWeightAndDegree = new Ordering[Tuple2[VertexId, Tuple2[Double, Int]]]() {
      override def compare(x: (VertexId, (Double, Int)), y: (VertexId, (Double, Int))): Int =
        if(x._2._1 > y._2._1 || x._2._1 < y._2._1) {
          Ordering[Double].compare(x._2._1, y._2._1)
        }else {
          Ordering[Int].compare(x._2._2, y._2._2)
        }
    }

    //Order vertex by degree and then weight
    val orderingByDegreeAndWeight = new Ordering[Tuple2[VertexId, Tuple2[Double, Int]]]() {
      override def compare(x: (VertexId, (Double, Int)), y: (VertexId, (Double, Int))): Int =
        if(x._2._2 > y._2._2 || x._2._2 < y._2._2) {
          Ordering[Int].compare(x._2._2, y._2._2)
        }else {
          Ordering[Double].compare(x._2._1, y._2._1)
        }
    }

    //choose the vertex with highest weight and degree
    var max_weight_degree_vertex: (VertexId, (Double, Int)) =
      degreeGraph.vertices.max()(orderingByWeightAndDegree)

    curr_vertices.append(max_weight_degree_vertex._1)
    curr_weight += max_weight_degree_vertex._2._1

    var nbRDD: VertexRDD[Array[VertexId]] = workingGraph.collectNeighborIds(EdgeDirection.Either)

    //initially, candidates is an Array which contains all neighbors of working vertex
    var candidates: Array[VertexId] = nbRDD.lookup(max_weight_degree_vertex._1)(0)

    while(!candidates.isEmpty && System.currentTimeMillis() - startTime < 3000) {
      workingGraph = workingGraph.subgraph(vpred = (vid, weight) => candidates contains vid)

      nbRDD = workingGraph.collectNeighborIds(EdgeDirection.Either)

      degreeGraph = workingGraph.outerJoinVertices(workingGraph.degrees)((vid, weight, degOpt) => (weight, degOpt.getOrElse(0)))

      //choose the vertex with highest weight and degree
      max_weight_degree_vertex =
        degreeGraph.vertices.max()(orderingByWeightAndDegree)

      curr_vertices.append(max_weight_degree_vertex._1)
      curr_weight += max_weight_degree_vertex._2._1

      //new candidates should be old candidates intersect neighbors of working vertex
      candidates = candidates.intersect(nbRDD.lookup(max_weight_degree_vertex._1)(0))
    }
    val costTime = System.currentTimeMillis() - startTime
    logInfo("Quickly get a mwcl: " + curr_vertices.toList + " " + curr_weight + " in " + costTime + " ms" )

    workingGraph.unpersist()
    degreeGraph.unpersist()

    MWCL(curr_vertices.toList, curr_weight)

  }

}

