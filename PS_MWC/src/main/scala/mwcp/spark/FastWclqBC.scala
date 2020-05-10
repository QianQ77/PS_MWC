package mwcp.spark

import mwcp.spark.MWCP.{MWCL, Subproblem}
import mwcp.spark.ReduceGraph.reduceGraphIter
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer

/**
  * Created by qiuqian on 3/28/18.
  */
object FastWclqBC extends LogHelper{

  val MAX_ITERATION: Int = 1


  def getRandomNumber(size: Int): Int = {
    scala.util.Random.nextInt(size)
  }

  //get the vertex(vid, benefit) which has higher benefit between two parameter
  def maxBenefitVID(v1: (VertexId, Double), v2: (VertexId, Double)): (VertexId, Double) = {
    if (v1._2 > v2._2) v1
    else v2
  }

  def getNeighborsForVid(neighborsMap_bc: Broadcast[Map[VertexId,
    Array[VertexId]]], vid: VertexId): Array[VertexId] = {
    neighborsMap_bc.value.getOrElse(vid, Array())
  }

  def getWeightForVid(weightMap_bc: Broadcast[Map[VertexId,
    Double]], vid: VertexId): Double = {
    weightMap_bc.value.getOrElse(vid, 0D)
  }

  // get benefit for each vertex; (vid, weight, benefit)
  // benefit(v) ∶= w(v)+  (w(Neighbors(v) ∩ CandSet))/2
  def getBenefit(vid: VertexId, neighborsMap_bc: Broadcast[Map[VertexId,
    Array[VertexId]]], weightMap_bc: Broadcast[Map[VertexId,
    Double]], candidatesSet: Buffer[VertexId]): (VertexId, Double) = {

    var weight = getWeightForVid(weightMap_bc, vid)

    var intersect = getNeighborsForVid(neighborsMap_bc, vid).intersect(candidatesSet)

    var benefit = {
      if (intersect.size > 0) intersect.map { case uid => getWeightForVid(weightMap_bc, uid) }.reduce(_ + _) / 2 + weight
      else weight
    }
    //logInfo(vid + " benefit: " + benefit)
    (vid, benefit)
  }

  // get sum of weights of the intersect of a vertex's neighbours and candidatesSet
  // upperbound(v) = w(v) + w(Neighbors(v) ∩ CandSet)
  // Calculates the upperbound of clique contains the vertex
  def getUpperBoundForVid(vid: VertexId, neighborsMap_bc: Broadcast[Map[VertexId,
    Array[VertexId]]], weightMap_bc: Broadcast[Map[VertexId,
    Double]], candidatesSet: Buffer[VertexId]): Double = {
    var intersect = getNeighborsForVid(neighborsMap_bc, vid).intersect(candidatesSet)

    if (intersect.size > 0)
      intersect.map {case uid => getWeightForVid(weightMap_bc, uid) }.reduce(_ + _) +
        getWeightForVid(weightMap_bc, vid)
    else getWeightForVid(weightMap_bc, vid)
  }

  /**
    * Tested
    * get a vertex with high benefit value
    *
    * @param candidatesSet
    * @param k
    * @return
    */
  def chooseAddVertex(neighborsMap_bc: Broadcast[Map[VertexId,
    Array[VertexId]]], weightMap_bc: Broadcast[Map[VertexId,
    Double]], candidatesSet: Buffer[VertexId], k: Int): VertexId = {

    //logInfo("Choosing AddVertex: " + candidatesSet)

    if (candidatesSet.size < k) {
      //benefit(v) ∶= w(v)+  (w(Neighbors(v) ∩ CandSet))/2
      var bestV: (VertexId, Double) =
        candidatesSet.map{
          case vid => getBenefit(vid, neighborsMap_bc, weightMap_bc, candidatesSet)
        }.reduce(maxBenefitVID)
      bestV._1
    } else {
      var randomIDs = (1 to k).map(_ => getRandomNumber(candidatesSet.size))
      var bestV: (VertexId, Double) = randomIDs.map(candidatesSet(_)).map{
        case vid => getBenefit(vid, neighborsMap_bc, weightMap_bc, candidatesSet)
      }.reduce(maxBenefitVID)
      bestV._1
    }
  }

  def reduceStartSet(startSet: Buffer[VertexId],
                     neighborsMap_bc: Broadcast[Map[VertexId, Array[VertexId]]],
                     weightMap_bc: Broadcast[Map[VertexId, Double]], curr_weight: Double): Buffer[VertexId] = {
    var original_startSet = startSet

    var reduced_startSet = Buffer[VertexId]()

    var sizeReduced: Int = Int.MaxValue

    while(original_startSet.size != 0 && sizeReduced > 20) {

      logInfo("Reducing StartSet")
      logInfo("Before Reducing, # of nodes: " +  original_startSet.size )

      val startTime = System.currentTimeMillis()

      var upperBoundSet: Buffer[Tuple2[VertexId, Double]] =
        original_startSet.map{case vid => (vid, getUpperBoundForVid(vid, neighborsMap_bc, weightMap_bc, original_startSet))}
      reduced_startSet =  upperBoundSet.filter(_._2 > curr_weight).map(_._1)

      sizeReduced = original_startSet.size - reduced_startSet.size
      original_startSet = reduced_startSet

      logInfo("After Reducing, # of nodes: " +  reduced_startSet.size )

    }
    reduced_startSet
  }


  /**
    *
    * @param subproblem
    * @param quickWeightBC   the current maximum weight clique we broadcast through machines
    * @return Option[MWCL] either None or a better value than the broadcast mwcl
    */
  def fastWClq_BC(subproblem: Subproblem, quickWeightBC: Broadcast[Double],
               weightMap_bc: Broadcast[Map[VertexId, Double]],
               neighborsMap_bc : Broadcast[Map[VertexId, Array[VertexId]]]): Option[MWCL] = {

    //logInfo("Start fastWClq on subproblem:" + subproblem);

    val startTime = System.currentTimeMillis

    val quickWeight = quickWeightBC.value

    var startSetSize: Int = 0

    // Calculate the sum of weights of vertex on partition path
    val partition_path_weight_sum: Double = subproblem.partition_path match {
      case Some(list) => list.map{ case vid => getWeightForVid(weightMap_bc, vid) }.reduce(_ + _)
      case None => 0D
    }

    var startSet: Buffer[VertexId] = subproblem.vertexSet.toBuffer

    //best MWCL we find in this subproblem
    var sub_mwcl = MWCL(Nil, 0)

    startSetSize = startSet.size

    //logInfo("Initial StartSet size: " + startSetSize);
    //logInfo("StartSet: " + startSet);

    if (startSetSize == 0) {
      None
    } else {

      val startSet_weight_sum: Double = startSet.map { case vid => getWeightForVid(weightMap_bc, vid) }.reduce(_ + _)

      if (startSet_weight_sum + partition_path_weight_sum <= quickWeight) {
        None

      } else {
        var iterations = 0

        while (iterations < MAX_ITERATION) {

          if(startSet.size == 0){
            //Start a new iteration
            startSet = subproblem.vertexSet.toBuffer
            iterations += 1
            startSetSize = startSet.size
          }
          var curr_vertices = ArrayBuffer[VertexId]()

          var curr_weight: Double = 0

          //get and remove a vertex in a generated random index in startSet
          var working_vertex: VertexId = startSet.remove(getRandomNumber(startSetSize))
          startSetSize -= 1

          //logInfo("working vertex: " + working_vertex)

          //add working_vertex id into current clique
          curr_vertices += working_vertex

          //add working_vertex weight into current clique weight
          curr_weight += getWeightForVid(weightMap_bc, working_vertex)

          //set candidatesSet to be neighbours of working vertex among startset
          var candidatesSet: Buffer[VertexId] = startSet.intersect(getNeighborsForVid(neighborsMap_bc, working_vertex)).toBuffer
          //logInfo("Candidates Set: "+ candidatesSet)

          // get the intersect of a vertex's neighbours and candidatesSet
          // Tested
          /*
          def getIntersectNborCand(vid: VertexId): Buffer[(VertexId, Double)] = getNeighborsForVid(vid).intersect(candidatesSet)
          */

          while (candidatesSet.size != 0) {

            var addingV: VertexId = chooseAddVertex(neighborsMap_bc, weightMap_bc, candidatesSet, 30)
            //logInfo("addingV: " + addingV)

            //Removes a single element addingV from this buffer
            candidatesSet -=  addingV

            //if w(currentClq)+ w(addingV)+ w(Neighbors(addingV)∩CandSet)+w(partitioning path) ≤ mwcl_weight then break
            if (curr_weight + getWeightForVid(weightMap_bc, addingV) +
              getUpperBoundForVid(addingV, neighborsMap_bc, weightMap_bc,  candidatesSet) + partition_path_weight_sum > quickWeight) {

              //add addingV into current clique
              curr_vertices += addingV
              //add addingV's weight into current clique weight
              curr_weight += getWeightForVid(weightMap_bc, addingV)

              //CandSet∶= Neighbors(addingV)∩CandSet
              candidatesSet = candidatesSet.intersect(getNeighborsForVid(neighborsMap_bc, addingV))

            }
          }
          //if current clique has higher weight than mwcl and sub_mwcl, update sub_mwcl to higher value and
          // use curr_weight to reduce graph
          if (curr_weight + partition_path_weight_sum > quickWeight && curr_weight + partition_path_weight_sum > sub_mwcl.weight) {

            var sub_mwcl_vertices = subproblem.partition_path match {
              case Some(list) => curr_vertices ++ list
              case None => curr_vertices
            }

            sub_mwcl = MWCL(sub_mwcl_vertices.toList, curr_weight + partition_path_weight_sum)

            logInfo("------------" + sub_mwcl.weight + " " + sub_mwcl.vertices)

            startSet = reduceStartSet(startSet, neighborsMap_bc, weightMap_bc, curr_weight)

            startSetSize = startSet.size

          }

        }
        //logInfo("Iterations:" + iterations)

        if (sub_mwcl.weight > quickWeight) Some(sub_mwcl)
        else None
      }
    }
  }
}


