package com.danolithe.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import java.util.Properties
import org.apache.spark.storage.StorageLevel
import java.util.ArrayList
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.util.TreeSet
import java.util.concurrent.ConcurrentSkipListSet
import java.util.HashMap


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
    
    var logger = Logger.getLogger(this.getClass())
    
    var id = 0
    var nodeNames = new HashMap[String, Long]()
    val edges: RDD[Edge[Long]] =
      sc.textFile(args(0)).map { line =>
        val fields:Array[String] = line.split(" ")
        
        val vertexId1 = nodeNames.get(fields[0])
        if(vertexId1 == null){
          nodeNames.put(fields[0], id)
          vertexId1 = id
          id = id+1
        }
         val vertexId2 = nodeNames.get(fields[0])
         if(vertexId2 == null){
          nodeNames.put(fields[1], id)
          vertexId2 = id
          id = id+1
        }
         
        Edge(vertexId1, vertexId2, fields(2).toLong)
    }
    var nodes: RDD[(VertexId, String)] = nodeNames.entrySet().map({})
    println(edges.count()) 
    
    val graph: Graph[(VertexId, String), Long] = Graph(edges, nodes)
    println("num edges = " + graph.numEdges);
    println("num vertices = " + graph.numVertices);   
    
    graph.vertices.saveAsTextFile(OUTPUT_PATH + "vertices")
    graph.edges.saveAsTextFile(OUTPUT_PATH + "edges")

  }
}
