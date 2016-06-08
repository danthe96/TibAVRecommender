package com.danolithe.spark

import java.util.Properties

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.util.concurrent.ConcurrentHashMap
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import scala.io.Source
import scala.collection.mutable.HashMap

object Main {

  var OUTPUT_PATH = {
    val prop = new Properties()
    val loader = Thread.currentThread().getContextClassLoader()
    val stream = loader.getResourceAsStream("version.txt")
    prop.load(stream);
    prop.getProperty("version") + "_output/"
  }

  def main(args: Array[String]) {
    /*if (args.length != 1) {
      System.err.println(
        "Should be one parameter: <path/to/edges>")
      System.exit(1)
    }*/

    val conf = new SparkConf()
      .setAppName("KnowMin-TIBAV")
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList)

    val sc = new SparkContext(conf)

    val logger = Logger.getLogger(this.getClass())

    var id: Int = 0
    var nodeNames = HashMap[String, Long]()
    var videoIds = Set[Long]()

    var typeEdges = List[Edge[Double]]()
    for (line <- Source.fromFile("../data/DBPedia_types_filtered_count.txt").getLines()) {
      val fields = line.split(" ")

      val vertexId1 = nodeNames.getOrElseUpdate(fields(0), {
        id += 1
        id - 1
      })
      val vertexId2 = nodeNames.getOrElseUpdate(fields(1), {
        id += 1
        id - 1
      })
      println(nodeNames.size)
      typeEdges :+(Edge(vertexId1, vertexId2, fields(2).toDouble))
      typeEdges :+(Edge(vertexId2, vertexId1, fields(2).toDouble))
    }
    
    for (line <- Source.fromFile("../data/gnd_DBpedia_filtered.txt").getLines()) {
      val fields = line.split(" ")

      val vertexId1 = nodeNames.getOrElseUpdate(fields(0), {
        id += 1
        id - 1
      })
      val vertexId2 = nodeNames.getOrElseUpdate(fields(1), {
        id += 1
        id - 1
      })
      println(nodeNames.size)
      typeEdges :+(Edge(vertexId1, vertexId2, 1))
      typeEdges :+(Edge(vertexId2, vertexId1, 1))
    }
    
    for (line <- Source.fromFile("../data/tib_gnd_sorted_count.txt").getLines()) {
      val fields = line.split(" ")

      val vertexId1 = nodeNames.getOrElseUpdate(fields(0), {
        id += 1
        id - 1
      })
      val vertexId2 = nodeNames.getOrElseUpdate(fields(1), {
        id += 1
        id - 1
      })
      
      videoIds = videoIds + (vertexId1)
      
      println(nodeNames.size)
      typeEdges :+(Edge(vertexId1, vertexId2, fields(2).toDouble / fields(3).toDouble))
      typeEdges :+(Edge(vertexId2, vertexId1, fields(2).toDouble / fields(3).toDouble))
    }

    var edges: RDD[Edge[Double]] = sc.parallelize(typeEdges)

    println("final nodes size" + nodeNames.size)

    val nodes: RDD[(VertexId, (String, List[(Double, Int)], List[Int]))] = sc.parallelize(nodeNames.toSeq.map { case (e1, e2) => (e2, (e1, List((0.0, -1)), List())) })
    val graph: Graph[(String, List[(Double, Int)], List[Int]), Double] = Graph(nodes, edges)

    val resultGraph = BFSRecommender.buildRecommenderGraph(graph)

    println("num edges = " + resultGraph.numEdges);
    println("num vertices = " + resultGraph.numVertices);

    resultGraph.vertices.saveAsTextFile(OUTPUT_PATH + "vertices")
    resultGraph.edges.saveAsTextFile(OUTPUT_PATH + "edges")
    
    //    val result = graph.pregel(BFSRecommender.initialMsg, 10, EdgeDirection.Out)(BFSRecommender.vprog, BFSRecommender.sendMsg, BFSRecommender.mergeMsg)
    //    result.vertices.saveAsTextFile(OUTPUT_PATH + "vertices")
    //    result.edges.saveAsTextFile(OUTPUT_PATH + "edges")

  }
}
