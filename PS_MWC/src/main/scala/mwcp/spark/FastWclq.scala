package mwcp.spark

import mwcp.spark.MWCP._
import mwcp.spark.ReduceGraphOld._
import mwcp.spark.ReduceGraph._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer

/**
  * Created by qiuqian on 3/21/18.
  */
object FastWclq extends LogHelper{


  def getRandomNumber(size: Int): Int = {
    scala.util.Random.nextInt(size)
  }

  //get the vertex(vid, weight, benefit) which has higher benefit between two parameter
  def maxBenefitVID(v1: (VertexId, Double, Double), v2: (VertexId, Double, Double)): (VertexId, Double, Double) = {
    if (v1._3 > v2._3) v1
    else v2
  }

  // get neighbours for a vertex with id == vid
  // Tested
  def getNeighborsForVid(neighboursRDD: VertexRDD[Array[(VertexId, Double)]],
                         vid: VertexId): Buffer[(VertexId, Double)] =
  neighboursRDD.filter(_._1 == vid).collect.head._2.toBuffer

  // Tested
  // get benefit for each vertex; (vid, weight, benefit)
  // benefit(v) ∶= w(v)+  (w(Neighbors(v) ∩ CandSet))/2
  def getBenefit(tuple: (VertexId, Double), neighboursRDD: VertexRDD[Array[(VertexId, Double)]], candidatesSet: Option[Buffer[(VertexId, Double)]]): (VertexId, Double, Double) = {
    var intersect = candidatesSet match {
      case Some(set) => getNeighborsForVid(neighboursRDD, tuple._1).intersect(set)
      case None => getNeighborsForVid(neighboursRDD, tuple._1)
    }
    var benefit = {
      if (intersect.size > 0) intersect.map { case (vid, weight) => weight }.reduce(_ + _) / 2 + tuple._2
      else tuple._2
    }
    (tuple._1, tuple._2, benefit)
  }

  // get sum of weights of the intersect of a vertex's neighbours and candidatesSet
  // w(Neighbors(addingV) ∩ CandSet)
  // this is used for calculating the upperbound of clique contains the vertex
  def getWeightSumIntersectNborCand(vid: VertexId, neighboursRDD: VertexRDD[Array[(VertexId, Double)]], candidatesSet: Buffer[(VertexId, Double)]): Double = {
    var intersect = getNeighborsForVid(neighboursRDD, vid).intersect(candidatesSet)
    if (intersect.size > 0) intersect.map { case (vid, weight) => weight }.reduce(_ + _)
    else 0
  }

  /**
    * Tested
    * get a vertex with high benefit value
    *
    * @param candidatesSet
    * @param k
    * @return
    */
  def chooseAddVertex(neighboursRDD: VertexRDD[Array[(VertexId, Double)]], candidatesSet: Buffer[(VertexId, Double)], k: Int): (VertexId, Double) = {

    //logInfo("Choosing AddVertex: " + candidatesSet)

    if(candidatesSet.size == 1){
      candidatesSet(0)

    }else if (candidatesSet.size < k) {
      //benefit(v) ∶= w(v)+  (w(Neighbors(v) ∩ CandSet))/2
      var bestV: (VertexId, Double, Double) = candidatesSet.map(getBenefit(_, neighboursRDD, Some(candidatesSet))).reduce(maxBenefitVID)
      (bestV._1, bestV._2)
    } else {
      var randomIDs = (1 to k).map(_ => getRandomNumber(candidatesSet.size))
      var bestV: (VertexId, Double, Double) = candidatesSet.zipWithIndex
        .filter(randomIDs contains _._2).map { case ((vid, weight), index) => (vid, weight) }.map(getBenefit(_, neighboursRDD, Some(candidatesSet))).reduce(maxBenefitVID)
      (bestV._1, bestV._2)
    }
  }

  // Not Used
  // Used for choosing working vertex
  // output: (vid, weight, benefit)
  def getBenefit(tuple: (VertexId, (Double, Array[(VertexId, Double)]))): (VertexId, Double, Double) = {
    if(tuple._2._2.size == 0) {
      (tuple._1, tuple._2._1, tuple._2._1)
    }else {
      var nbsWeightSum = tuple._2._2.map{case (vid, weight) => weight}.reduce(_ + _)
      (tuple._1, tuple._2._1, tuple._2._1 + nbsWeightSum/2)
    }
  }

  // Not Used
  def chooseWorkingVertex(neighboursRDDWithSelfWeight: VertexRDD[(Double, Array[(VertexId, Double)])], k: Int): (VertexId, Double) = {

    logInfo("Choosing Working Vertex: ")

    var rddSize:Long = neighboursRDDWithSelfWeight.count()

    if (rddSize < k) {
      //benefit(v) ∶= w(v)+  w(Neighbors(v))/2
      var bestV: (VertexId, Double, Double) =
        neighboursRDDWithSelfWeight.map(getBenefit(_)).reduce(maxBenefitVID)
      (bestV._1, bestV._2)
    } else {
      var randomIDs = (1 to k).map(_ => getRandomNumber(rddSize.toInt))
      var bestV: (VertexId, Double, Double) = neighboursRDDWithSelfWeight.zipWithIndex
        .filter(randomIDs contains _._2).map(_._1).map(getBenefit(_)).reduce(maxBenefitVID)
      (bestV._1, bestV._2)
    }
  }

  /**
    *
    * @param subgraph
    * @param cutOff the maximum run time for one subgraph, unit is second
    * @param mwcl   the current maximum weight clique we broadcast through machines
    * @return Option[MWCL] either None or a better value than the broadcast mwcl
    */
  def fastWClq(subgraph: Subgraph, cutOff: Int, mwcl: MWCL): Option[MWCL] = {

    logInfo("Start fastWClq on subgraph:" + subgraph);

    val startTime = System.currentTimeMillis

    // Calculate the sum of weights of vertex on partition path
    val partition_path_weight_sum = subgraph.partition_path match {
      case Some(list) => list.map(v => v._2).reduce(_ + _)
      case None => 0
    }

    //inputGraph is val, but we will reduce the graph, so we define a mutable graph
    var graph = subgraph.graph

    //best MWCL we find in this subgraph
    var sub_mwcl = MWCL(Nil, 0)

    var startSet: Buffer[(VertexId, Double)] = graph.vertices.collect().toBuffer
    logInfo("Initial StartSet size: " + startSet.size);
    //logInfo("StartSet: " + startSet);

    if (startSet.size == 0) {
      None
    } else {

      val startSet_weight_sum = startSet.map { case (uid, weight) => weight }.reduce(_ + _)

      if (startSet_weight_sum + partition_path_weight_sum <= mwcl.weight) {
        None

      } else {
        var iterations = 0

        while (!startSet.isEmpty && System.currentTimeMillis - startTime < cutOff * 1000) {

          iterations += 1

          var curr_vertices = ArrayBuffer[VertexId]()

          var curr_weight: Double = 0

          graph.cache()

          //We should generate new neighbourRDD because the graph might have been reduced
          var neighboursRDD = graph.collectNeighbors(EdgeDirection.Either)

          neighboursRDD.cache()
          //logInfo("neighboursRDD:" + neighboursRDD.foreach(println))


          /*
          var neighboursRDDWithSelfWeight: VertexRDD[(Double, Array[(VertexId, Double)])] =
            graph.outerJoinVertices(neighboursRDD)((vid, weight, nbs) =>
              nbs match {
                case Some(array) => (weight, array)
                case None => (weight, Array[(VertexId, Double)]())
              }).vertices.filter(startSet contains _._1)
              */
          /*
          var neighboursRDDWithSelfWeight: VertexRDD[(Double, Array[(VertexId, Double)])] =
            neighboursRDD.leftJoin(graph.vertices)((vid, nbs, weight) =>
              weight match {
                case Some(value) => (value, nbs)
                case None => (0, nbs)
              })
          neighboursRDDWithSelfWeight.cache()
          */


          //get and remove a vertex in a generated random index in startSet, takes linear time
          var working_vertex: (VertexId, Double) = startSet.remove(getRandomNumber(startSet.size))

          //var working_vertex: (VertexId, Double) = chooseWorkingVertex(neighboursRDDWithSelfWeight, 60)
          startSet -= working_vertex

          //logInfo("working vertex: " + working_vertex)

          //add working_vertex id into current clique
          curr_vertices += working_vertex._1

          //add working_vertex weight into current clique weight
          curr_weight += working_vertex._2

          //set candidatesSet to be neighbours of working vertex
          var candidatesSet: Buffer[(VertexId, Double)] = getNeighborsForVid(neighboursRDD, working_vertex._1)

          //remove working_vertex from neighboursRDD
          neighboursRDD = neighboursRDD.filter(_._1 != working_vertex._1)
          //neighboursRDDWithSelfWeight = neighboursRDDWithSelfWeight.filter(_._1 != working_vertex._1)
          graph = graph.subgraph(vpred = (vid, weight) => vid != working_vertex._1)

          // get the intersect of a vertex's neighbours and candidatesSet
          // Tested
          /*
          def getIntersectNborCand(vid: VertexId): Buffer[(VertexId, Double)] = getNeighborsForVid(vid).intersect(candidatesSet)
          */

          while (candidatesSet.size != 0) {

            var addingV: (VertexId, Double) = chooseAddVertex(neighboursRDD, candidatesSet, 30)
            //logInfo("addingV: " + addingV)

            //Removes a single element addingV from this buffer, at its first occurrence.
            candidatesSet -= addingV

            //if w(currentClq)+ w(addingV)+ w(Neighbors(addingV)∩CandSet)+w(partitioning path) ≤ mwcl_weight then break
            if (curr_weight + addingV._2 +
              getWeightSumIntersectNborCand(addingV._1, neighboursRDD, candidatesSet) + partition_path_weight_sum > mwcl.weight) {

              //add addingV into current clique
              curr_vertices += addingV._1
              //add working_vertex weight into current clique weight
              curr_weight += addingV._2

              //CandSet∶= Neighbors(addingV)∩CandSet
              candidatesSet = getNeighborsForVid(neighboursRDD, addingV._1).intersect(candidatesSet)

            }
          }
          //if current clique has higher weight than mwcl and sub_mwcl, update sub_mwcl to higher value and
          // use curr_weight to reduce graph
          if (curr_weight + partition_path_weight_sum > mwcl.weight && curr_weight + partition_path_weight_sum > sub_mwcl.weight) {

            var sub_mwcl_vertices = subgraph.partition_path match {
              case Some(list) => curr_vertices ++ list.map { case (vid: VertexId, weight: Double) => vid }
              case None => curr_vertices
            }

            sub_mwcl = MWCL(sub_mwcl_vertices.toList, curr_weight + partition_path_weight_sum)

            logInfo("------------" + sub_mwcl.weight + " " + sub_mwcl.vertices)

            //graph = reduceGraph(graph, neighboursRDD, curr_weight)
            graph = reduceGraphIter(graph, curr_weight)

            neighboursRDD.unpersist()

            startSet = graph.vertices.collect().toBuffer

          }

        }
        logInfo("Iterations:" + iterations)

        if (sub_mwcl.weight > mwcl.weight) Some(sub_mwcl)
        else None
        }
    }
  }
}


