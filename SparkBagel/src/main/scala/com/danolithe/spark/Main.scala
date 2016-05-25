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
import org.apache.spark.storage.StorageLevel
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
    if (args.length != 1) {
      System.err.println(
        "Should be one parameter: <path/to/edges>")
      System.exit(1)
    }

    val conf = new SparkConf()
      .setAppName("KnowMin-TIBAV")
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList)
    val sc = new SparkContext(conf)

    val logger = Logger.getLogger(this.getClass())

    var id = 0
    var nodeNames = HashMap[String, Long]()
    val edges: RDD[Edge[Long]] =
      sc.textFile(args(0)).map { line =>
        val fields: Array[String] = line.split(" ")

        val vertexId1 = nodeNames.getOrElseUpdate(fields(0), {
          id += 1
          id - 1
        })
        val vertexId2 = nodeNames.getOrElseUpdate(fields(1), {
          id += 1
          id - 1
        })

        Edge(vertexId1, vertexId2, fields(2).toLong)
      }
    val nodes: RDD[(VertexId, String)] = sc.parallelize(nodeNames.toSeq.map { case (e1, e2) => (e2, e1) })
    println(edges.count())

    val graph: Graph[String, Long] = Graph(nodes, edges)
    println("num edges = " + graph.numEdges);
    println("num vertices = " + graph.numVertices);

    graph.vertices.saveAsTextFile(OUTPUT_PATH + "vertices")
    graph.edges.saveAsTextFile(OUTPUT_PATH + "edges")

  }
}
