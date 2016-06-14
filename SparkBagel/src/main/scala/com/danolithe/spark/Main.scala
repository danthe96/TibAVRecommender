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
    if (args.length != 1) {
      System.err.println(
        "Should be one parameter: <video_id>")
      System.exit(1)
    }
    val video_id = args(0)

    val conf = new SparkConf()
      .setAppName("KnowMin-TIBAV")
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList)

    val sc = new SparkContext(conf)

    var id: Int = 0
    var nodeNames = HashMap[String, Long]()
    var videoIds = Set[Long]()

    var typeEdges = List[Edge[Double]]()
    //for (line <- Source.fromFile("../data/DBPedia_types_filtered_count.txt").getLines()) {
    for (line <- Source.fromFile("../data/test1b/t1_types_filtered_sorted_count.txt").getLines()) {
      val fields = line.split(" ")

      val vertexId1 = nodeNames.getOrElseUpdate(fields(0), {
        id += 1
        id - 1
      })
      val vertexId2 = nodeNames.getOrElseUpdate(fields(1), {
        id += 1
        id - 1
      })
      typeEdges = typeEdges :+ (Edge(vertexId1, vertexId2, fields(2).toDouble/fields(3).toDouble))
      typeEdges = typeEdges :+ (Edge(vertexId2, vertexId1, fields(2).toDouble/fields(3).toDouble))
    }

    //    for (line <- Source.fromFile("../data/gnd_DBpedia_filtered.txt").getLines()) {
    for (line <- Source.fromFile("../data/test1b/t1_gnd_dbp_filtered_sorted_count.txt").getLines()) {
      val fields = line.split(" ")

      val vertexId1 = nodeNames.getOrElseUpdate(fields(0), {
        id += 1
        id - 1
      })
      val vertexId2 = nodeNames.getOrElseUpdate(fields(1), {
        id += 1
        id - 1
      })
      typeEdges = typeEdges :+ (Edge(vertexId1, vertexId2, 1.0))
      typeEdges = typeEdges :+ (Edge(vertexId2, vertexId1, 1.0))
      //typeEdges :+
    }

    //    for (line <- Source.fromFile("../data/tib_gnd_sorted_count.txt").getLines()) {
    for (line <- Source.fromFile("../data/test1b/t1_tib_gnd_filtered_sorted_count_1.txt").getLines()) {
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

      typeEdges = typeEdges :+ (Edge(vertexId1, vertexId2, fields(2).toDouble / fields(3).toDouble))
      typeEdges = typeEdges :+ (Edge(vertexId2, vertexId1, fields(2).toDouble / fields(3).toDouble))

    }

    for (line <- Source.fromFile("../data/test1b/t1_pagelinks_filtered_sorted_count.txt").getLines()) {
    val fields = line.split(" ")

    val vertexId1 = nodeNames.getOrElseUpdate(fields(0), {
        id += 1
        id - 1
      })
      val vertexId2 = nodeNames.getOrElseUpdate(fields(1), {
        id += 1
        id - 1
      })
      typeEdges = typeEdges :+ (Edge(vertexId1, vertexId2, 0.1 * (fields(2).toDouble/fields(3).toDouble)))
    }
    
    var edges: RDD[Edge[Double]] = sc.parallelize(typeEdges)

    val nodes: RDD[(VertexId, (String, Set[(Double, Int, List[String])], Set[Long], Boolean, Boolean))] = sc.parallelize(nodeNames.toSeq.map { case (e1, e2) => (e2, (e1, Set[(Double, Int, List[String])](), Set[Long](), e1 == video_id, videoIds.contains(e2))) })
    val graph: Graph[(String, Set[(Double, Int, List[String])], Set[Long], Boolean, Boolean), Double] = Graph(nodes, edges)

    val resultGraph = BFSRecommender.buildRecommenderGraph(graph)
    
    resultGraph.vertices.filter(node => (videoIds.contains(node._1) && node._1 != video_id)).foreach(println)

    /*var recommendScores: Array[(Long, String, Double)] = resultGraph.vertices.toArray().foldLeft((0: Int, 0: Double, 0: Int)){
        case ((a, b, c), m) => (
          a + m.get("a").collect{case i: Int => i}.getOrElse(0),
          b + m.get("b").collect{case i: Double => i}.getOrElse(0),
          c + m.get("c").collect{case i: Int => i}.getOrElse(0)
          )
        }
    */
    /*var recommendScores: Array[(Long, String, Double)] = resultGraph.vertices.map[(Long, String, Double)](node => {
      val aggr = node._2._2.aggregate((0.0, 0))((acc, value) => (acc._1 + value._1, acc._2 + value._2), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      if (aggr._2 == 0) {
        (node._1, node._2._1, -0.0)
      } else {
        (node._1, node._2._1, aggr._1 / aggr._2)
      }
    }).collect()*/
    var recommendScores: Array[(Long, String, Double)] = resultGraph.vertices.map[(Long, String, Double)](node => {
      val aggr:Double = node._2._2.foldLeft(0.0){(value:Double,el) => value+ (el._1 /el._2)}
      if (aggr == 0) {
        (node._1, node._2._1, -0.0)
      } else {
        (node._1, node._2._1, aggr)
      }
    }).collect()
    recommendScores = recommendScores.filter(score => (videoIds.contains(score._1) && score._2 != video_id)).sortBy(-_._3)
    recommendScores.indices.foreach(i => { println((i + 1) + ". " + recommendScores(i)._2 + ", Score: " + recommendScores(i)._3) })


//    resultGraph.vertices.saveAsTextFile(OUTPUT_PATH + "vertices.txt")
//    resultGraph.edges.saveAsTextFile(OUTPUT_PATH + "edges.txt")

    //    val result = graph.pregel(BFSRecommender.initialMsg, 10, EdgeDirection.Out)(BFSRecommender.vprog, BFSRecommender.sendMsg, BFSRecommender.mergeMsg)
    //    result.vertices.foreach(println)
    //result.vertices.foreach(println)
    //    result.vertices.saveAsTextFile(OUTPUT_PATH + "vertices")
    //    result.edges.saveAsTextFile(OUTPUT_PATH + "edges")

  }
}
