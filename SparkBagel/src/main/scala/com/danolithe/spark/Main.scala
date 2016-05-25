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
    
    var videoMap:Map[String, Long] = Map()
    var index:Long = 0

    val edges: RDD[Edge[Double]] =
      sc.textFile("../data/input.txt").flatMap { line =>
        val fields = line.split(" ")
        
        if(videoMap.contains(fields(0)) == false) {
          videoMap += (fields(0) -> index)
          index += 1
        }
        
        if(videoMap.contains(fields(1)) == false) {
          videoMap += (fields(1) -> index)
          index += 1
        }
        
        List(
            Edge(videoMap.apply(fields(0)), videoMap.apply(fields(1)), 1/(fields(2).toDouble)),
            Edge(videoMap.apply(fields(0)), videoMap.apply(fields(1)), 1/(fields(2).toDouble))
            )
      }

    val graph: Graph[Any, Double] = Graph.fromEdges(edges, 0.0)

    println("num edges = " + graph.numEdges);
    println("num vertices = " + graph.numVertices);

    graph.vertices.saveAsObjectFile(OUTPUT_PATH)
    graph.edges.saveAsObjectFile(OUTPUT_PATH)

  }
}
