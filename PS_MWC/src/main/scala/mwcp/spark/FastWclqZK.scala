package mwcp.spark

import mwcp.spark.MWCP.{MWCL, Subproblem}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.{WatchedEvent, ZooKeeper}
import org.apache.spark.util.LongAccumulator

import scala.collection.Map
import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer
import scala.tools.cmd.Spec.Accumulator

/**
  * Created by qiuqian on 3/28/18.
  */

object FastWclqZK extends LogHelper with Watcher with Serializable{

  val MAX_ITERATION: Int = 1

  var zk: ZooKeeper = null
  var zk_mwcl:Double  = 0

  /*
  var zk: ZooKeeper = ZKHelper.startZK(this, MWCP.ZKConfig.zkserverIP)
  var zk_mwcl:Double  = ZKHelper.readWeightFromZookeeper(zk, this)
  */

  //Once the value of Znode changes, this watcher receives an watchedEvent
  //If new value > current value, then update value on this watcher
  override def process(watchedEvent: WatchedEvent) = {
    val newValue: Double = ZKHelper.readWeightFromZookeeper(zk, this)
    val oldValue: Double = zk_mwcl
    if (newValue > zk_mwcl) {
      zk_mwcl = newValue
      logInfo("Update zk_mwcl on watcher, before: " + oldValue + ", after: " + zk_mwcl)
    }
  }

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


  // upperbound(v) = w(C) + w(v) + w(Neighbors(v) ∩ CandSet)
  // Calculates the upperbound of weight of any clique extended from C by adding v and more vertices
  def getUpperBoundForVid(curr_clique_weight: Double, vid: VertexId, neighborsMap_bc: Broadcast[Map[VertexId,
    Array[VertexId]]], weightMap_bc: Broadcast[Map[VertexId,
    Double]], candidatesSet: Buffer[VertexId]): Double = {
    var intersect = getNeighborsForVid(neighborsMap_bc, vid).intersect(candidatesSet)

    if (intersect.size > 0)
      intersect.map {case uid => getWeightForVid(weightMap_bc, uid) }.reduce(_ + _) +
        getWeightForVid(weightMap_bc, vid) + curr_clique_weight
    else getWeightForVid(weightMap_bc, vid) + curr_clique_weight
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
                     weightMap_bc: Broadcast[Map[VertexId, Double]], optimal_weight: Double): Buffer[VertexId] = {
    var original_startSet = startSet

    var reduced_startSet = Buffer[VertexId]()

    var sizeReduced: Int = Int.MaxValue

    while(original_startSet.size != 0 && sizeReduced > 20) {

      //logInfo("Reducing StartSet")
      //logInfo("Before Reducing, # of nodes: " +  original_startSet.size )

      var upperBoundSet: Buffer[Tuple2[VertexId, Double]] =
        original_startSet.map{case vid => (vid, getUpperBoundForVid(0, vid, neighborsMap_bc, weightMap_bc, original_startSet))}
      reduced_startSet =  upperBoundSet.filter(_._2 > optimal_weight).map(_._1)

      sizeReduced = original_startSet.size - reduced_startSet.size
      original_startSet = reduced_startSet

      //logInfo("After Reducing, # of nodes: " +  reduced_startSet.size )

    }
    reduced_startSet
  }


  /**
    *
    * @param subproblem
    * @param weightMap_bc   the weightMap we broadcast through machines
    * @param neighborsMap_bc the neighborsMap we broadcast through machines
    * @return Option[MWCL] either None or a better value than the value of Zookeeper Node: /MWCL
    */
  def fastWClq_ZK(zooKeeperServerIp: String, subproblem: Subproblem,
               weightMap_bc: Broadcast[Map[VertexId, Double]],
               neighborsMap_bc : Broadcast[Map[VertexId, Array[VertexId]]],
                  reducedSubSolveStep: LongAccumulator,
                  solvedSubProbemNumber: LongAccumulator,
                  solvedSubProbemNumber_halfPruned: LongAccumulator): Option[MWCL] = {

    //logInfo("Start fastWClq on subproblem:" + subproblem);

    if(zk == null) {
      zk = ZKHelper.startZK(this, zooKeeperServerIp)
      zk_mwcl = ZKHelper.readWeightFromZookeeper(zk, this)
      logInfo("Maximum weight in zk: " + zk_mwcl)
    }


    val startTime = System.currentTimeMillis

    // Calculate the sum of weights of vertex on partition path
    val partition_path_weight_sum: Double = subproblem.partition_path match {
      case Some(list) => list.map{ case vid => getWeightForVid(weightMap_bc, vid) }.reduce(_ + _)
      case None => 0D
    }

    var startSet: Buffer[VertexId] = subproblem.vertexSet.toBuffer

    //logInfo("Start solving StartSet: " + startSet.size)

    //best MWCL we find in this subproblem
    var sub_mwcl = MWCL(Nil, 0)

    var startSetSize: Int = startSet.size


    //logInfo("Initial StartSet size: " + startSetSize);
    //logInfo("StartSet: " + startSet);

    if (startSetSize == 0) {
      //logInfo("Finish solving StartSet: " + startSet.size)
      None
    } else {

      if (subproblem.upperBound <= ZKHelper.readWeightFromZookeeper(zk, this)) {
        reducedSubSolveStep.add(1)
        //logInfo("Finish solving StartSet: " + startSet.size)
        None

      } else {
        val originalStartSetSize = startSetSize
        // This variable is used to calculate solvedSubProbemNumber_halfPruned
        // once current startSetSize <= originalStartSetSize,
        // set halfPruned = true and add 1 to solvedSubProbemNumber_halfPruned
        var halfPruned: Boolean = false

        var iterations = 0
        startSet = reduceStartSet(startSet, neighborsMap_bc, weightMap_bc, zk_mwcl - partition_path_weight_sum)
        startSetSize = startSet.size
        if(startSetSize == 0) {
          reducedSubSolveStep.add(1)
        }else {
          solvedSubProbemNumber.add(1)
          if(!halfPruned && startSetSize <= originalStartSetSize / 2) {
            halfPruned = true
            solvedSubProbemNumber_halfPruned.add(1)
          }
        }

        //old zk_mwcl value we used to reduce the StartSet
        var old_zk_mwcl:Double = zk_mwcl

        while (iterations < MAX_ITERATION && startSet.size > 0) {

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
          var candidatesSet: Buffer[VertexId] = startSet.intersect(getNeighborsForVid(neighborsMap_bc, working_vertex))
          //logInfo("Candidates Set: "+ candidatesSet)

          // get the intersect of a vertex's neighbours and candidatesSet
          // Tested
          /*
          def getIntersectNborCand(vid: VertexId): Buffer[(VertexId, Double)] = getNeighborsForVid(vid).intersect(candidatesSet)
          */

          var continue = true
          while (candidatesSet.size != 0 && continue) {

            var addingV: VertexId = chooseAddVertex(neighborsMap_bc, weightMap_bc, candidatesSet, 30)
            //logInfo("addingV: " + addingV)

            //Removes a single element addingV from this buffer
            candidatesSet -=  addingV

            //if w(currentClq)+ w(addingV)+ w(Neighbors(addingV)∩CandSet)+w(partitioning path) ≤ mwcl_weight then break
            if (getUpperBoundForVid(curr_weight, addingV, neighborsMap_bc, weightMap_bc,  candidatesSet) +
              partition_path_weight_sum > zk_mwcl) {

              //add addingV into current clique
              curr_vertices += addingV
              //add addingV's weight into current clique weight
              curr_weight += getWeightForVid(weightMap_bc, addingV)

              //CandSet∶= Neighbors(addingV)∩CandSet
              candidatesSet = candidatesSet.intersect(getNeighborsForVid(neighborsMap_bc, addingV))

            }else {
              continue = false
            }

          }
          //if current clique has higher weight than zk_mwcl, update sub_mwcl to higher value and
          // update zk_mwcl
          if (curr_weight + partition_path_weight_sum > zk_mwcl) {

            var sub_mwcl_vertices = subproblem.partition_path match {
              case Some(list) => curr_vertices ++ list
              case None => curr_vertices
            }

            sub_mwcl = MWCL(sub_mwcl_vertices.toList, curr_weight + partition_path_weight_sum)
            logInfo("------------" + sub_mwcl.weight + " " + sub_mwcl.vertices)
            zk_mwcl = ZKHelper.updateZookeeper(zk, this, sub_mwcl.weight)

          }

          if(startSet.size == 0){
            //Start a new iteration
            //logInfo("Start a new iteration")
            startSet = subproblem.vertexSet.toBuffer
            iterations += 1
            startSetSize = startSet.size
          }else if (old_zk_mwcl < zk_mwcl) {
            //if zk_mwcl is updated since last reduceStartSet,
            // then we should reduce StartSet with the updated value
            startSet = reduceStartSet(startSet, neighborsMap_bc, weightMap_bc, zk_mwcl - partition_path_weight_sum)
            startSetSize = startSet.size
            old_zk_mwcl = zk_mwcl

            if(!halfPruned && startSetSize <= originalStartSetSize / 2) {
              halfPruned = true
              solvedSubProbemNumber_halfPruned.add(1)
            }
          }

        }
        //logInfo("Iterations:" + iterations)
        //zk.close()

        //logInfo("Finish solving StartSet: " + startSet.size)
        if (sub_mwcl.weight >= zk_mwcl) Some(sub_mwcl)
        else None
      }
    }
  }
}


