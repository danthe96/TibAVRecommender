package com.danolithe.spark

import java.util.Properties

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

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
    var nodeNames : RDD[(String, Long)] = sc.emptyRDD[(String, Long)](scala.reflect.classTag[(String, Long)]);
    
    //var nodeNames : RDD[(String, Long)] = sc.parallelize(Seq(("test", 0)))
    
    
    var id : Long = 0
    var videoIds = Set[Long]()
    val typeEdges: RDD[Edge[Double]] =
      sc.textFile("../data/DBPedia_types_filtered_count.txt").flatMap { line =>
        val fields = line.split(" ")
        
        var vertexId1 : Long = 0
        var vertexId2 : Long = 0
        
        if (nodeNames.lookup(fields(0)) != null) {
          vertexId1 = nodeNames.lookup(fields(0)).get(0)
        }
        else {
          vertexId1 = nodeNames.top(1).last._2 + 1
          nodeNames ++ sc.makeRDD(Seq((fields(0), vertexId1)))
        }
        if (nodeNames.lookup(fields(1)) != null) {
          vertexId2 = nodeNames.lookup(fields(1)).get(0)
        }
        else {
          vertexId2 = nodeNames.top(1).last._2 + 1
          nodeNames ++ sc.makeRDD(Seq((fields(1), vertexId2)))
        }
                
        List(
          Edge(vertexId1, vertexId2, fields(2).toDouble),
          Edge(vertexId2, vertexId1, fields(2).toDouble))
      }
    println(nodeNames.count())
    

    val dbpediaEdges: RDD[Edge[Double]] =
      sc.textFile("../data/gnd_DBpedia_filtered.txt").flatMap { line =>
        val fields = line.split(" ")

       var vertexId1 : Long = 0
        var vertexId2 : Long = 0
        
                if (nodeNames.lookup(fields(0)) != null) {
          vertexId1 = nodeNames.lookup(fields(0)).get(0)
        }
        else {
          vertexId1 = nodeNames.top(1).last._2 + 1
          nodeNames ++ sc.makeRDD(Seq((fields(0), vertexId1)))
        }
        if (nodeNames.lookup(fields(1)) != null) {
          vertexId2 = nodeNames.lookup(fields(1)).get(0)
        }
        else {
          vertexId2 = nodeNames.top(1).last._2 + 1
          nodeNames ++ sc.makeRDD(Seq((fields(1), vertexId2)))
        }
                
        List(
          Edge(vertexId1, vertexId2, 1),
          Edge(vertexId2, vertexId1, 1))
      }
   

    val videoEdges: RDD[Edge[Double]] =
      sc.textFile("../data/tib_gnd_sorted_count.txt").flatMap { line =>
        val fields = line.split(" ")

        var vertexId1 : Long = 0
        var vertexId2 : Long = 0
        
                if (nodeNames.lookup(fields(0)) != null) {
          vertexId1 = nodeNames.lookup(fields(0)).get(0)
        }
        else {
          vertexId1 = nodeNames.top(1).last._2 + 1
          nodeNames ++ sc.makeRDD(Seq((fields(0), vertexId1)))
        }
        if (nodeNames.lookup(fields(1)) != null) {
          vertexId2 = nodeNames.lookup(fields(1)).get(0)
        }
        else {
          vertexId2 = nodeNames.top(1).last._2 + 1
          nodeNames ++ sc.makeRDD(Seq((fields(1), vertexId2)))
        }

        videoIds = videoIds + (vertexId1)

        List(
          Edge(vertexId1, vertexId2, fields(2).toDouble / fields(3).toDouble),
          Edge(vertexId2, vertexId1, fields(2).toDouble / fields(3).toDouble))
      }

    var edges: RDD[Edge[Double]] = typeEdges
    edges = edges ++ dbpediaEdges
    edges = edges ++ videoEdges
    val nodes: RDD[(VertexId, (String, List[(Double, Int)], List[Int]))] = sc.parallelize(nodeNames.collect.map { 
      case (e1, e2) => (e2, (e1, List((0.0, -1)), List[Int]()))
      })
    val graph: Graph[(String, List[(Double, Int)], List[Int]), Double] = Graph(nodes, edges)

    //val resultGraph = BFSRecommender.buildRecommenderGraph(graph)

    println("num edges = " + graph.numEdges);
    println(nodeNames.count())
    println("num vertices = " + graph.numVertices);

    //resultGraph.vertices.saveAsTextFile(OUTPUT_PATH + "vertices")
    //resultGraph.edges.saveAsTextFile(OUTPUT_PATH + "edges")

    //    val result = graph.pregel(BFSRecommender.initialMsg, 10, EdgeDirection.Out)(BFSRecommender.vprog, BFSRecommender.sendMsg, BFSRecommender.mergeMsg)
    //    result.vertices.saveAsTextFile(OUTPUT_PATH + "vertices")
    //    result.edges.saveAsTextFile(OUTPUT_PATH + "edges")

  }
}
