package mwcp.spark


import mwcp.spark.GetQuickMWCL._
import mwcp.spark.ReduceGraph._
import mwcp.spark.FastWclqBC._
import mwcp.spark.FastWclqZK._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, VertexId, _}
import org.apache.spark.rdd.RDD
import org.apache.log4j._
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}

import scala.collection.Map

/**
  * Created by qiuqian on 2/14/18.
  */
object MWCP extends LogHelper with Watcher{


  Logger.getLogger("org").setLevel(Level.ERROR)

  Logger.getLogger("mwcp.spark").setLevel(Level.ALL)

  case class Subgraph(partition_path: Option[List[(VertexId, Double)]] , graph: Graph[Double, Int])

  // The partition_path contains a clique,
  // The upperBound denotes the max weight of clique this subproblem could contain
  // the vertexSet contains higher-ordering neighbors of all vertices in partition_path
  case class Subproblem(partition_path: Option[List[VertexId]], upperBound: Double, vertexSet: Set[VertexId])

  // The key of subproblem, upperBound is the sum of weights of this vertex and its higher-order neighbors
  case class SubproblemKey(vid: VertexId, upBound: Double)

  // sort by upperBound in desc order
  object SubproblemKey {
    implicit def orderingByUpBound[SK <: SubproblemKey] : Ordering[SK] = {
      Ordering.by(sk => sk.upBound * -1)
    }
  }

  case class SubproblemKey2(vid: VertexId, setSize: Int)
  // sort by size of higher-ordering neighbors set in desc order
  object SubproblemKey2 {
    implicit def orderingBySetSize[SK <: SubproblemKey2] : Ordering[SK] = {
      Ordering.by(sk => sk.setSize * -1)
    }
  }

  object ZKConfig {var zkserverIP: String = _}

  // Current Maximum Weight Clique
  case class MWCL(vertices: List[VertexId] , weight: Double)

  def createSimpleGraph(sc: SparkContext): Graph[Double, Int] = {
    val vertexArray: Array[(Long, Double)] = Array((1L, 11), (2L, 30),
      (3L, 16), (4L, 9), (5L, 13), (6L, 25), (7L, 19), (8L, 10.5), (9L, 23),
      (10L, 30), (11L, 50))

    val vertexRDD: RDD[(Long, Double)] = sc.parallelize(vertexArray)

    // Create an RDD for edges
    val edgesRDD: RDD[Edge[Int]] =
      sc.parallelize(Array(Edge(1L, 2L, 1), Edge(1L, 3L, 1), Edge(1L, 4L, 1), Edge(1L, 5L, 1),
        Edge(1L, 7L, 1), Edge(1L, 8L, 1), Edge(2L, 3L, 1), Edge(2L, 4L, 1), Edge(2L, 5L, 1), Edge(2L, 6L, 1),
        Edge(3L, 4L, 1), Edge(3L, 5L, 1), Edge(3L, 8L, 1), Edge(3L, 9L, 1), Edge(3L, 10L, 1),
        Edge(4L, 5L, 1), Edge(4L, 6L, 1), Edge(4L, 8L, 1),
        Edge(5L, 10L, 1), Edge(8L, 9L, 1), Edge(10L, 11L, 1)))

    // Create the graph
    Graph(vertexRDD, edgesRDD)
  }

  def generateGraphFromInput(sc: SparkContext, inputPath: String, edgeParNum: Int): Graph[Double, Int] = {

    val originalGraph = GraphLoader.edgeListFile(sc, inputPath, false, edgeParNum)

      //set the weight of ith vertex to be (i mod 200) + 1
    originalGraph.mapVertices { case (vid, defaultValue) => (vid % 200 + 1).toDouble }

  }


  def main(args: Array[String]): Unit = {

    val isLocal:Boolean = args.length == 0

    val conf = isLocal match {
      case true => new SparkConf().setAppName("MWCP_SPARK").setMaster("local[4]").set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
      case false => new SparkConf().setAppName("MWCP_SPARK").set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
    }

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")
    conf.registerKryoClasses(Array(classOf[ZKHelper],classOf[Subproblem], classOf[Array[scala.Option[_]]],
      classOf[MWCL], classOf[SubproblemKey], classOf[SubproblemKey2],
      Class.forName("org.apache.spark.graphx.impl.VertexAttributeBlock"),
      Class.forName("org.apache.spark.graphx.impl.ShippableVertexPartition"),
      Class.forName("org.apache.spark.util.collection.OpenHashSet"),
      Class.forName("org.apache.spark.util.collection.OpenHashSet$LongHasher"),
      Class.forName("org.apache.spark.util.collection.BitSet"),
      Class.forName("scala.math.Ordering"),
      Class.forName("scala.math.Ordering$$anon$4"),
      Class.forName("scala.math.Ordering$DoubleOrdering$$anon$2"),
      Class.forName("scala.math.Ordering$$anon$5"),
      Class.forName("scala.math.Ordering$Double$"),
      Class.forName("mwcp.spark.MWCP"),
      Class.forName("mwcp.spark.MWCP$$anonfun$main$8"),
      Class.forName("scala.reflect.ManifestFactory$$anon$10"),
      Class.forName("scala.reflect.ClassTag$$anon$1"),
      Class.forName("scala.reflect.ManifestFactory$$anon$12"),
      Class.forName("java.lang.Class"),
      Class.forName("org.apache.spark.graphx.impl.RoutingTablePartition"),
      Class.forName("org.apache.spark.graphx.impl.EdgePartition$mcI$sp"),
      Class.forName("scala.reflect.ManifestFactory$$anon$9"),
      Class.forName("org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap$mcJI$sp"),
      Class.forName("org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap$$anonfun$1"),
      Class.forName("org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap$$anonfun$2")
    ))

    val sc = new SparkContext(conf)

    logInfo("Default Parallelism = " + sc.defaultParallelism)

    val inputPath = isLocal match {
      case true => "data/web-uk-2005.mtx"
      //case true => "data/updated_gene1"
      case false => args(0)
    }

    //0: partition based on degree and then vertex id; using Zookeeper;
    //1: withOutPartition
    //2: partition based on vertex id
    //3: not using Zookeeper
    val experimentID = isLocal match {
    case true => "0"
    case false => args(1)
    }

    val zooKeeperServerIp = isLocal match {
      case true => "localhost"
      case false => args(2)
    }

    //reduce the graph after getQuickMWCL or not
    val ifReduce = isLocal match {
      case true => false
      case false => args(3).toBoolean
    }

    //the number of partitions of edgesRDD
    val edgeParNum = isLocal match {
      case true => 8
      case false => args(4).toInt
    }

    //mapPartition experimentID
    //0: get results using map insteadOf mapPartition; do not repartitionAndSortWithinPartitions
    //1: get results using mapPartition; do not repartitionAndSortWithinPartitions
    //2: get results using mapPartition; use repartitionAndSortWithinPartitions and sort by upperBound
    //3: get results using mapPartition; use repartitionAndSortWithinPartitions and sort by size of HNbSet
    var mapParExperimentID = isLocal match {
      case true => 2
      case false => args(5).toInt
    }

    var weightedGraph = generateGraphFromInput(sc, inputPath, edgeParNum).cache()
      //.partitionBy(PartitionStrategy.CanonicalRandomVertexCut, sc.defaultParallelism)

    val originalVerticesNum = weightedGraph.numVertices
    val originalEdgesNum = weightedGraph.numEdges

    val startTime = System.currentTimeMillis()

    val quickMwcl: MWCL = getQuickMwcl(weightedGraph)

    //number of sub-problems discarded at the begining of solving step, without constructing a clique
    val reducedSubSolveStep = sc.longAccumulator("reducedSubSolveStep")

    //number of sub-problems discarded in partitioning step
    var reducedSubPar: Long = 0

    var reducedSubTotal: Long = 0

    //number of sub-problems which are solved at solving step
    val solvedSubProbemNumber = sc.longAccumulator("solvedSubProbemNumber")

    //number of su-problems which are pruned more than a half vertices at solving step
    val solvedSubProbemNumber_halfPruned = sc.longAccumulator("solvedSubProbemNumber_halfPruned")

    //If we use ZooKeeper
    if(!experimentID.equals("3")) {
      ZKConfig.zkserverIP = zooKeeperServerIp
      val zk: ZooKeeper = ZKHelper.startZK(this, zooKeeperServerIp)
      ZKHelper.initializeZNode(zk, quickMwcl.weight)
    }


    if(ifReduce) {
      //weightedGraph.unpersist()
      weightedGraph = reduceGraph(weightedGraph, quickMwcl.weight).cache()
      //weightedGraph.cache()
    }

    //logInfo("number of partitions of verticesRDD: " + weightedGraph.vertices.partitions.length)
    //logInfo("number of partitions of edgesRDD: " + weightedGraph.edges.partitions.length)

    var weightMap_bc: Broadcast[Map[VertexId, Double]] = sc.broadcast(weightedGraph.vertices.collectAsMap())

    var neighborsMap_bc: Broadcast[Map[VertexId, Array[VertexId]]] =
      sc.broadcast(weightedGraph.collectNeighborIds(EdgeDirection.Either).collectAsMap())

    var result: Option[MWCL] = null

    if(experimentID.equals("1")){
      //If we don't divide weightedGraph into subProblems
      val wholeProblem = Subproblem(None, Double.MaxValue, weightedGraph.vertices.map(_._1).collect.toSet)

      result = fastWClq_ZK(zooKeeperServerIp, wholeProblem,
        weightMap_bc, neighborsMap_bc, reducedSubSolveStep, solvedSubProbemNumber, solvedSubProbemNumber_halfPruned)

    }else{
      //generate subproblems each of which contain a vertex id and
      // a list contains higher-ordering neighbors for this vertex

      /*
      val subProblems = Partition.partition(weightedGraph, quickMwcl.weight, experimentID.equals("2")).cache().map{
        case (vid, (weight, hnbSet)) =>
          Subproblem(Some(List(vid)), hnbSet)
      }


      logInfo("number of partitions of subProblems: " + subProblems.partitions.length)

      subProblems.cache()
      */

      /*

      val subProblems2 = Partition.partition(weightedGraph, quickMwcl.weight, experimentID.equals("2")).map{
        case (vid, (weight, hnbSet)) =>
          (SubproblemKey(vid, hnbSet.size), hnbSet)
      }.repartitionAndSortWithinPartitions(new HashPartitioner(edgeParNum))
      */


      /* statistic of sizes of sub-problems
      val subProblemStat = Partition2.partition(weightedGraph, zooKeeperServerIp).map{
        case (vid, (upBound, hbnSet)) => hbnSet.size
      }.cache()
      val numSub = subProblemStat.count()
      logInfo("number of sub-problems: " + numSub)
      logInfo("Max size of sub-problems: " + subProblemStat.max())
      logInfo("Average size of sub-problems: " + subProblemStat.reduce(_ + _)/numSub)
      subProblemStat.unpersist()
      */

      val subProblems3 = Partition2.partition(weightedGraph, zooKeeperServerIp).map{
        case (vid, (upBound, hnbSet)) =>
          Subproblem(Some(List(vid)), upBound, hnbSet)
      }.cache()

      val subProblems4 = Partition2.partition(weightedGraph, zooKeeperServerIp).map{
        case (vid, (upBound, hnbSet)) =>
          (SubproblemKey(vid, upBound), hnbSet)
      }.repartitionAndSortWithinPartitions(new HashPartitioner(edgeParNum)).cache()

      val subProblems5 = Partition2.partition(weightedGraph, zooKeeperServerIp).map{
        case (vid, (upBound, hnbSet)) =>
          (SubproblemKey2(vid, hnbSet.size), (upBound, hnbSet))
      }.repartitionAndSortWithinPartitions(new HashPartitioner(edgeParNum)).cache()


      if(experimentID.equals("3")){
        /* not using Zookeeper
        * */
        // Broadcast mwcl
        //val mwcl: Broadcast[MWCL] = sc.broadcast(MWCL(List(weightedGraph.vertices.max._1), weightedGraph.vertices.max._2))
        val mwclWeight: Broadcast[Double] = sc.broadcast(quickMwcl.weight)

        result = subProblems3.
          map(_subproblem => fastWClq_BC(_subproblem, mwclWeight, weightMap_bc, neighborsMap_bc)).
          reduce(maxMWCL(_, _))

      }else {

        /*cost 31s,40s on web-uk-2005 when run on local[4]
        * */
        /*
        result = subProblems3.
          map(_subproblem => fastWClq_ZK(zooKeeperServerIp, _subproblem, weightMap_bc, neighborsMap_bc)).
          filter(_ != None).reduce(maxMWCL(_, _))
          */


        def solvePerPartition(iter: Iterator[Subproblem]):Iterator[Option[MWCL]] = {
          var res = for {
            e <- iter
            clq = fastWClq_ZK(zooKeeperServerIp, e, weightMap_bc, neighborsMap_bc,
              reducedSubSolveStep, solvedSubProbemNumber, solvedSubProbemNumber_halfPruned)
            if clq != None
          } yield clq
          res
        }

        def solvePerPartition2(iter: Iterator[Tuple2[SubproblemKey, Set[VertexId]]]):Iterator[Option[MWCL]] = {
          var res = for {
            e <- iter
            clq = fastWClq_ZK(zooKeeperServerIp, Subproblem(Some(List(e._1.vid)), e._1.upBound, e._2),
              weightMap_bc, neighborsMap_bc, reducedSubSolveStep, solvedSubProbemNumber, solvedSubProbemNumber_halfPruned)
            if clq != None
          } yield clq
          res
        }

        def solvePerPartition5(iter: Iterator[(SubproblemKey2, (Double, Set[VertexId]))]):Iterator[Option[MWCL]] = {
          var res = for {
            e <- iter
            clq = fastWClq_ZK(zooKeeperServerIp, Subproblem(Some(List(e._1.vid)), e._2._1, e._2._2),
              weightMap_bc, neighborsMap_bc, reducedSubSolveStep, solvedSubProbemNumber, solvedSubProbemNumber_halfPruned)
            if clq != None
          } yield clq
          res
        }


        //mapPartition experimentID
        //0: get results using map insteadOf mapPartition; do not repartitionAndSortWithinPartitions
        //1: get results using mapPartition; do not repartitionAndSortWithinPartitions
        //2: get results using mapPartition; use repartitionAndSortWithinPartitions and sort by upperBound
        //3: get results using mapPartition; use repartitionAndSortWithinPartitions and sort by size of HNbSet
        result = mapParExperimentID match {
          case 0 =>
            reducedSubPar = originalVerticesNum - subProblems3.count()
            subProblems3.map(_subproblem =>
            fastWClq_ZK(zooKeeperServerIp, _subproblem, weightMap_bc, neighborsMap_bc,
              reducedSubSolveStep, solvedSubProbemNumber, solvedSubProbemNumber_halfPruned)).
            filter(_ != None).reduce(maxMWCL(_, _))
          case 1 =>
            reducedSubPar = originalVerticesNum - subProblems3.count()
            subProblems3.mapPartitions(solvePerPartition).reduce(maxMWCL(_, _))
          case 2 =>
            reducedSubPar = originalVerticesNum - subProblems4.count()
            subProblems4.mapPartitions(solvePerPartition2).reduce(maxMWCL(_, _))
          case 3 =>
            reducedSubPar = originalVerticesNum - subProblems5.count()
            subProblems5.mapPartitions(solvePerPartition5).reduce(maxMWCL(_, _))
        }

        logInfo("Reduced " + reducedSubPar + " subProblems while partitioning")

        //for web-uk-2005, the count = 13; cost 180s
        /*
        val count = subProblems.mapPartitions(solvePerPartition).count()
        logInfo("count of result: "+ count)
        */

        /*cost 58s on web-uk-2005 when run on local[4]
        * */
        //result = subProblems.mapPartitions(solvePerPartition).coalesce(1).reduce(maxMWCL(_, _))

        /*cost 32s on web-uk-2005 when run on local[4]
        * */
        //result = subProblems3.mapPartitions(solvePerPartition).coalesce(1).reduce(maxMWCL(_, _))

        /*cost 299s on web-uk-2005 when run on local[4]
        * */
        //result = subProblems3.mapPartitions(solvePerPartition).coalesce(4).reduce(maxMWCL(_, _))

        /*cost 106s on web-uk-2005 when run on local[4]
        * */
        //result = subProblems3.mapPartitions(solvePerPartition).reduce(maxMWCL(_, _))

        /*cost 45s on web-uk-2005 when run on local[4]
        * */
        //result = subProblems4.mapPartitions(solvePerPartition2).reduce(maxMWCL(_, _))

        /*cost 47s on web-uk-2005 when run on local[4]
        cost 44s on web-uk-2005 when set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
        * */
        //result = subProblems5.mapPartitions(solvePerPartition5).reduce(maxMWCL(_, _))

        /*cost 56s on web-uk-2005 when run on local[4]
        * */
        //result = subProblems4.mapPartitions(solvePerPartition2).coalesce(1).reduce(maxMWCL(_, _))

        /*cost 56s on web-uk-2005 when run on local[4]
       * */
        //result = subproblems2.mapPartitions(solvePerPartition2).coalesce(1).reduce(maxMWCL(_, _))

        /*cost 65s~152s on web-uk-2005 when run on local[4]
        * */
        //result = subProblems.mapPartitions(solvePerPartition).repartition(4).reduce(maxMWCL(_, _))

        /*cost 282s on web-uk-2005 when run on local[4]
        * */
        //result = subProblems.mapPartitions(solvePerPartition).repartition(8).reduce(maxMWCL(_, _))

        /*cost 213s on web-uk-2005 when run on local[4]
        * */
        //result = subProblems.repartition(8).mapPartitions(solvePerPartition).reduce(maxMWCL(_, _))

        //result = subproblems2.mapPartitions(solvePerPartition2).reduce(maxMWCL(_, _))

        /*cost 280s on web-uk-2005 when run on local[4]
        * */
        //result = subProblems.mapPartitions(solvePerPartition).takeOrdered(1)(Ordering[Double].reverse.on(_.get.weight))(0)

        /*cost 388s on web-uk-2005 when run on local[4]
        * */
        //result = subProblems.mapPartitions(solvePerPartition).collect().reduce(maxMWCL(_, _))



        //cost 64s on web-uk-2005 when run on local[4] when using Set in message
        //cost 38s on web-uk-2005 when run on local[4] when using Buffer in message

        //result = subProblems3.mapPartitions(solvePerPartition).reduce(maxMWCL(_, _))

        /*
        resultRDD.partitions.size: 8
        resultRDD.count: 12
        newResultRDD.partitions.size: 1
        cost 277s on web-uk-2005 when run on local[4]


        val resultRDD = subProblems.mapPartitions(solvePerPartition)
        resultRDD.cache()
        logInfo("resultRDD.partitions.size: " + resultRDD.partitions.length)
        logInfo("resultRDD.count: " + resultRDD.count())

        val newResultRDD = resultRDD.coalesce(coalesceNum)
        logInfo("newResultRDD.partitions.size: " + newResultRDD.partitions.length)

        result = newResultRDD.reduce(maxMWCL(_, _))
        */



      }

    }


    var resultMWCL:MWCL = result match {
      case None =>   quickMwcl
      case Some(mwcl_result) => mwcl_result
    }

    val resultString = "Maximum Weight Clique: " + resultMWCL.vertices + ", Weight: " + resultMWCL.weight
    logInfo(resultString)

    val timeCost = System.currentTimeMillis() - startTime
    val timeCostString = "Time Cost: " + timeCost + " ms"
    logInfo(timeCostString)

    logInfo("original vertices num: " + originalVerticesNum)
    logInfo("reducedSubPar: " + reducedSubPar)
    logInfo("reducedSubSolveStep: " + reducedSubSolveStep.sum)
    reducedSubTotal = reducedSubPar + reducedSubSolveStep.sum
    logInfo("reducedSubTotal: " + reducedSubTotal)
    logInfo("solvedSubProbemNumber: " + solvedSubProbemNumber.sum)
    logInfo("solvedSubProbemNumber_halfPruned: " + solvedSubProbemNumber_halfPruned.sum)


    val experimentString = inputPath.split("/").last + "_" + experimentID
    logInfo(experimentString)

    sc.stop()


  }

  def maxMWCL(mwcl1: Option[MWCL], mwcl2: Option[MWCL]): Option[MWCL] = {
    logInfo("mwcl1:" + mwcl1.get.weight)
    logInfo("mwcl2:" + mwcl2.get.weight)
    if(mwcl1.get.weight > mwcl2.get.weight) mwcl1 else mwcl2
    /*
    mwcl1 match {
      case None => mwcl2
      case Some(result1) =>
        mwcl2 match  {
          case None => mwcl1
          case Some(result2) => if(result1.weight > result2.weight) mwcl1 else mwcl2
        }
    }*/
  }

  override def process(watchedEvent: WatchedEvent) = {
    None
  }

}
