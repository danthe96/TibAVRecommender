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
    
    var entityMap:HashMap[String, Long] = new HashMap()
    var index:Long = 0

    val typeEdges: RDD[Edge[Double]] =
      sc.textFile("../data/DBPedia_types_filtered_count.txt").flatMap { line =>
        val fields = line.split(" ")
        
        if(entityMap.contains(fields(0)) == false) {
          entityMap += (fields(0) -> index)
          index += 1
        }
        
        if(entityMap.contains(fields(1)) == false) {
          entityMap += (fields(1) -> index)
          index += 1
        }
        
        List(
            Edge(entityMap.apply(fields(0)), entityMap.apply(fields(1)), 1/(fields(2).toDouble)),
            Edge(entityMap.apply(fields(1)), entityMap.apply(fields(0)), 1/(fields(2).toDouble))
            )
      }
    
      val dbpediaEdges: RDD[Edge[Double]] =
      sc.textFile("../data/gnd_DBpedia_filtered.txt").flatMap { line =>
        val fields = line.split(" ")
        
        if(entityMap.contains(fields(0)) == false) {
          entityMap += (fields(0) -> index)
          index += 1
        }
        
        if(entityMap.contains(fields(1)) == false) {
          entityMap += (fields(1) -> index)
          index += 1
        }
        
        List(
            Edge(entityMap.apply(fields(0)), entityMap.apply(fields(1)), 1),
            Edge(entityMap.apply(fields(1)), entityMap.apply(fields(0)), 1)
            )
      }
      
      val videoEdges: RDD[Edge[Double]] =
      sc.textFile("../data/tib_gnd_sorted_count.txt").flatMap { line =>
        val fields = line.split(" ")
        
        if(entityMap.contains(fields(0)) == false) {
          entityMap += (fields(0) -> index)
          index += 1
        }
        
        if(entityMap.contains(fields(1)) == false) {
          entityMap += (fields(1) -> index)
          index += 1
        }
        
        List(
            Edge(entityMap.apply(fields(0)), entityMap.apply(fields(1)), fields(2).toDouble/fields(3).toDouble),
            Edge(entityMap.apply(fields(1)), entityMap.apply(fields(0)), fields(2).toDouble/fields(3).toDouble)
            )
      }

    
    var edges: RDD[Edge[Double]] = typeEdges
    edges ++ dbpediaEdges
    edges ++ videoEdges
    val graph: Graph[Any, Double] = Graph.fromEdges(edges, 0.0)


    println("num edges = " + graph.numEdges);
    println("num vertices = " + graph.numVertices);   
    
    graph.vertices.saveAsTextFile(OUTPUT_PATH + "vertices")
    graph.edges.saveAsTextFile(OUTPUT_PATH + "edges")

  }
}
