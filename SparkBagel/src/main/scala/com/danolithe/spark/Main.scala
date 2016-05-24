package com.danolithe.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import java.util.Properties
import org.apache.spark.storage.StorageLevel

object Main {

  var OUTPUT_PATH = {
    val prop = new Properties()
    val loader = Thread.currentThread().getContextClassLoader()
    val stream = loader.getResourceAsStream("version.txt")
    prop.load(stream);
    prop.getProperty("version") + "_output"
  }

  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println(
        "Should be one parameter: <path/to/edges>")
      System.exit(1)
    }

    var logger = Logger.getLogger(this.getClass())

    val conf = new SparkConf()
      .setAppName("Load graph")
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList)

    val sc = new SparkContext(conf)

    val edges: RDD[Edge[String]] =
      sc.textFile(args(0)).map { line =>
        val fields = line.split(" ")
        Edge(fields(0).toLong, fields(1).toLong, fields(2))
      }

    val graph: Graph[Any, String] = Graph.fromEdges(edges, "defaultProperty")

    println("num edges = " + graph.numEdges);
    println("num vertices = " + graph.numVertices);

    graph.vertices.saveAsObjectFile(OUTPUT_PATH)
    graph.edges.saveAsObjectFile(OUTPUT_PATH)

  }
}
