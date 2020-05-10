package util

import mwcp.spark.LogHelper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by qiuqian on 4/19/18.
  * Generate Graph For Scalability Experiment
  */
object GenerateGraphForExperiment extends LogHelper{

  Logger.getLogger("org").setLevel(Level.ERROR)

  Logger.getLogger("mwcp.spark").setLevel(Level.ALL)

  def main(args: Array[String]): Unit = {
    //val conf = new SparkConf().setAppName("MWCP_SPARK").setMaster("local[4]")
    val conf = new SparkConf().setAppName("MWCP_SPARK")
    val sc = new SparkContext(conf)

    //val inputPath = "data/ca-dblp-2010.mtx"
    //val outputPath = "data/experiment/ca-dblp-2010-"

    val inputPath = args(0)
    val outputPath = args(1)

    var i = 1

    val originalGraph = GraphLoader.edgeListFile(sc, inputPath).cache()

    val vertexNum = originalGraph.numVertices
    logInfo("Original graph's vertices number: "+ vertexNum)
    logInfo("Original graph's edges number: "+ originalGraph.numEdges)

    while (i < 5) {
      var maxVertexId = i * vertexNum / 5
      logInfo(i + " maxVertexId: " + maxVertexId)

      originalGraph.edges.filter(edge => edge.dstId <= maxVertexId && edge.srcId <= maxVertexId).
        map(edge => edge.srcId + " " + edge.dstId).saveAsTextFile(outputPath + i.toString)

      val graph = GraphLoader.edgeListFile(sc, outputPath + i.toString)
      logInfo("Graph" + i + " vertices number: " + graph.numVertices)
      logInfo("Graph" + i + " edges number: " + graph.numEdges)

      i += 1
    }


  }

}
