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

object NodeType extends Enumeration {
    type NodeType = Value
    val VIDEO, GND, DBPEDIA, YAGO = Value
}

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
    var gndIds = Set[Long]()
    var dbpIds = Set[Long]()
    var yagoIds = Set[Long]()
    var dboIds = Set [Long]()

    var typeEdges = List[Edge[Double]]()/*
    for (line <- Source.fromFile("../data/Filtered/DBPedia_types_filtered_sorted_count.txt")("UTF-8").getLines()) {
    //for (line <- Source.fromFile("../data/test1b/t1_types_filtered_sorted_count.txt").getLines()) {
      val fields = line.split(" ")

      val vertexId1 = nodeNames.getOrElseUpdate(fields(0), {
        id += 1
        id - 1
      })
      val vertexId2 = nodeNames.getOrElseUpdate(fields(1), {
        id += 1
        id - 1
      })
      
      dboIds = dboIds + (vertexId2)
      
      typeEdges = typeEdges :+ (Edge(vertexId1, vertexId2, (1.0/3.0)*1.0))
      typeEdges = typeEdges :+ (Edge(vertexId2, vertexId1, 1.0/fields(2).toDouble))
    }
    
    println("finished importing DBPedia Types")
*/
    for (line <- Source.fromFile("../data/Filtered/GND_DBPEDIA_filtered_sorted_count.txt")("UTF-8").getLines()) {
    //for (line <- Source.fromFile("../data/test1b/t1_gnd_dbp_filtered_sorted_count.txt").getLines()) {
      val fields = line.split(" ")

      val vertexId1 = nodeNames.getOrElseUpdate(fields(0), {
        id += 1
        id - 1
      })
      val vertexId2 = nodeNames.getOrElseUpdate(fields(1), {
        id += 1
        id - 1
      })
      
      gndIds = gndIds + (vertexId1)
      dbpIds = dbpIds + (vertexId2)
      
      typeEdges = typeEdges :+ (Edge(vertexId1, vertexId2, 1.0))
      typeEdges = typeEdges :+ (Edge(vertexId2, vertexId1, 1.0))
      //typeEdges :+
    }
    
    println("finished importing GND-DBPedia")

    for (line <- Source.fromFile("../data/Filtered/tib_gnd_sorted_count_with_gnd.txt")("UTF-8").getLines()) {
    //for (line <- Source.fromFile("../data/test1b/t1_tib_gnd_filtered_sorted_count_1.txt").getLines()) {
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
      gndIds = gndIds + (vertexId2)

      typeEdges = typeEdges :+ (Edge(vertexId1, vertexId2, fields(2).toDouble / fields(3).toDouble))
      typeEdges = typeEdges :+ (Edge(vertexId2, vertexId1, fields(2).toDouble / fields(3).toDouble))

    }
    
    println("finished importing TIB-GND")
    
    
    for (line <- Source.fromFile("../data/Filtered/yago_types_filtered_count.txt")("UTF-8").getLines()) {
    //for (line <- Source.fromFile("../data/test1b/t1_tib_gnd_filtered_sorted_count_1.txt").getLines()) {
      val fields = line.split(" ")

      val vertexId1 = nodeNames.getOrElseUpdate(fields(0), {
        id += 1
        id - 1
      })
      val vertexId2 = nodeNames.getOrElseUpdate(fields(1), {
        id += 1
        id - 1
      })
      
      dbpIds = dbpIds + (vertexId1)
      yagoIds = yagoIds + (vertexId2)

      typeEdges = typeEdges :+ (Edge(vertexId1, vertexId2, (1.0/3.0)*(1 / fields(2).toDouble)))
      typeEdges = typeEdges :+ (Edge(vertexId2, vertexId1, 1 / fields(3).toDouble))

    }

    println("finished importing YAGO Types")
    /*
    for (line <- Source.fromFile("../data/Filtered/yago_supertypes_filtered_sorted_count.txt")("UTF-8").getLines()) {
    //for (line <- Source.fromFile("../data/test1b/t1_tib_gnd_filtered_sorted_count_1.txt").getLines()) {
      val fields = line.split(" ")

      val vertexId1 = nodeNames.getOrElseUpdate(fields(0), {
        id += 1
        id - 1
      })
      val vertexId2 = nodeNames.getOrElseUpdate(fields(1), {
        id += 1
        id - 1
      })
      
      yagoIds = yagoIds + (vertexId1)
      yagoIds = yagoIds + (vertexId2)

      typeEdges = typeEdges :+ (Edge(vertexId1, vertexId2, 1 / fields(2).toDouble))
      typeEdges = typeEdges :+ (Edge(vertexId2, vertexId1, 1 / fields(3).toDouble))

    }

    println("finished importing YAGO super types")
*/
   /* for (line <- Source.fromFile("../data/test1b/t1_pagelinks_filtered_sorted_count.txt").getLines()) {
    val fields = line.split(" ")

    val vertexId1 = nodeNames.getOrElseUpdate(fields(0), {
        id += 1
        id - 1
      })
      val vertexId2 = nodeNames.getOrElseUpdate(fields(1), {
        id += 1
        id - 1
      })
      typeEdges = typeEdges :+ (Edge(vertexId1, vertexId2, 0.5 * (1.0/fields(2).toDouble)))
    }*/
    
    var edges: RDD[Edge[Double]] = sc.parallelize(typeEdges)

    // RDD[(ID, (name, Set[(path_len, path_nodecount, path_nodenames, path_type)], visited, isTarget, nodeType)))]
    val nodes: RDD[(VertexId, (String, Set[(Double, List[String])], Set[Long], Boolean, String))] = sc.parallelize(nodeNames.toSeq.map (e => {
        if(videoIds.contains(e._2)){
           (e._2, (e._1, Set[(Double, List[String])](), Set[Long](), e._1 == video_id, "VIDEO")) 
        } else if (gndIds.contains(e._2)){
           (e._2, (e._1, Set[(Double, List[String])](), Set[Long](), e._1 == video_id, "GND"))
        } else if (dbpIds.contains(e._2)){
          (e._2, (e._1, Set[(Double, List[String])](), Set[Long](), e._1 == video_id, "DBPEDIA"))
        } else{
          (e._2, (e._1, Set[(Double, List[String])](), Set[Long](), e._1 == video_id, "YAGO"))
        }
    }))
    val graph: Graph[(
        String,           //Node name 
        Set[(             //Set for paths
          Double,         //Path weight
          List[String])], //Pathlist
        Set[Long],        //Set of allready reached nodes
        Boolean,          //Start node
        String),          //Node type
        Double            //Edge weight
        ] = Graph(nodes, edges)

    val resultGraph = BFSRecommender.buildRecommenderGraph(graph)
    
    graph.unpersist(blocking = false)
    resultGraph.cache()
    
    /*resultGraph.vertices.filter(node => (videoIds.contains(node._1) && node._2._1 != video_id)).foreach(node => {
      println("paths found from " + video_id + " to " + node._2._1 + ": ")
      node._2._2.foreach(println)
    })*/

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
      val aggr:Double = node._2._2.foldLeft(0.0){(value:Double,el) => value + el._1}
      if (aggr == 0) {
        (node._1, node._2._1, -0.0)
      } else {
        (node._1, node._2._1, aggr)
      }
    }).collect().filter(score => (videoIds.contains(score._1) && score._2 != video_id)).sortBy(-_._3).take(20);
    
    resultGraph.unpersist(blocking = false)
    graph.cache()
    
    println()
    println()
    println("Applying Jaccard Similarity...")
    
    val bfsGraph = graph.mapVertices((vertexId, vd)  => Double.PositiveInfinity)
    println("Prepared bfs graph...")
    
    graph.unpersist(blocking = false)
    bfsGraph.cache()
    
    val sourceRDD = BFS.buildBfsGraph(bfsGraph.mapVertices((vertexId, vd) => {
      if(vertexId != nodeNames.get(video_id).get)
        vd
      else {
          println("Taged source video")
          0.0
        }
    })).vertices.filter(vertexVal => vertexVal._2 != Double.PositiveInfinity)
    println(sourceRDD.count + ", bfsVertices: " + bfsGraph.vertices.count)
    
    recommendScores.foreach(item => {
      val targetRDD = BFS.buildBfsGraph(bfsGraph.mapVertices((vertexId, vd) => {
        if(vertexId != item._1)
          vd
        else {
          println("Taged video Id: " + item._1)
          0.0
        }
      })).vertices.filter(vertexVal => vertexVal._2 != Double.PositiveInfinity)
      val jaccardSimilarityA : Double = (sourceRDD.intersection(targetRDD)).count.toDouble
      val jaccardSimilarityB : Double = (sourceRDD.union(targetRDD)).count.toDouble
      println("Intersection size: "+jaccardSimilarityA+", Union size: "+jaccardSimilarityB) 
      println("Jaccard Similarity of " + item._2 + ": " + (jaccardSimilarityA/jaccardSimilarityB))
    })
    
    
    
    
    println()
    println()
    println("Scores:")    
    
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
