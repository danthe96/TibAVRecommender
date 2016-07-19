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
import java.io.FileWriter

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

  var nodeNames = HashMap[String, Long]()

  var videoIds = Set[Long]()
  var gndIds = Set[Long]()
  var dbpIds = Set[Long]()
  var yagoIds = Set[Long]()
  var dboIds = Set[Long]()
  var linkToIds = Set[Long]()

  var typeEdges = List[Edge[Double]]()

  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println(
        "Should be one parameter: <video_id>")
      System.exit(1)
    }
    //    val video_id = args(0)

    val conf = new SparkConf()
      .setAppName("KnowMin-TIBAV")
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList)
    val sc = new SparkContext(conf)
    val edges: RDD[Edge[Double]] = input(sc)

    for ((video_id, vertexId) <- nodeNames) {
      if (videoIds.contains(vertexId)) {
        print("Running pregel for Video ID " + video_id)

        // RDD[(ID, (name, Set[(path_len, path_nodecount, path_nodenames, path_type)], visited, isTarget, nodeType)))]
        val nodes: RDD[(VertexId, (String, Set[(Double, List[(String, String)])], Set[Long], Boolean, String))] = sc.parallelize(nodeNames.toSeq.map(e => {
          if (videoIds.contains(e._2)) {
            (e._2, (e._1, Set[(Double, List[(String, String)])](), Set[Long](), e._1 == video_id, "VIDEO"))
          } else if (gndIds.contains(e._2)) {
            (e._2, (e._1, Set[(Double, List[(String, String)])](), Set[Long](), e._1 == video_id, "GND"))
          } else if (dbpIds.contains(e._2)) {
            (e._2, (e._1, Set[(Double, List[(String, String)])](), Set[Long](), e._1 == video_id, "DBPEDIA"))
          } else {
            (e._2, (e._1, Set[(Double, List[(String, String)])](), Set[Long](), e._1 == video_id, "YAGO"))
          }
        }))
        val graph: Graph[(String, //Node name 
        Set[( //Set for paths
        Double, //Path weight
        List[(String, String)])], //Pathlist (Name, Type)
        Set[Long], //Set of already reached nodes
        Boolean, //Start node
        String), //Node type
        Double //Edge weight
        ] = Graph(nodes, edges)

        val resultGraph = BFSRecommender.buildRecommenderGraph(graph)
        graph.unpersist(blocking = false)
        resultGraph.cache()
        val bfsGraph = graph.mapVertices((vertexId, vd) => (Int.MaxValue, vd._5))
        println("Prepared bfs graph...")
        bfsGraph.cache()

        output(video_id, resultGraph, bfsGraph)
      }
    }
  }

  def output(video_id: String, resultGraph: Graph[(String, Set[(Double, List[(String, String)])], Set[Long], Boolean, String), Double], bfsGraph: Graph[(Int, String), Double]) {
    var recommendationScores: Array[(Long, String, Double, Set[(Double, List[(String, String)])])] = resultGraph.vertices.map[(Long, String, Double, Set[(Double, List[(String, String)])])](node => {
      val aggr: Double = node._2._2.foldLeft(0.0) {
        (value: Double, el) =>
          {
            val semanticCount: Double = el._2.count(_._2 == "YAGO")
            if (semanticCount >= 1)
              value + (el._1 * (1.0 + 1.0 / (semanticCount)))
            else
              value + el._1
          }
      }
      if (aggr == 0) {
        (node._1, node._2._1, -0.0, node._2._2)
      } else {
        (node._1, node._2._1, aggr, node._2._2)
      }
    }).collect().filter(score => (videoIds.contains(score._1) && score._2 != video_id)).sortBy(-_._3).take(20);

    var recommendationScoresFiltered: Array[(Long, String, Double)] = recommendationScores.map(node => (node._1, node._2, node._3));

    resultGraph.unpersist(blocking = false)

    println()
    println()
    println("Applying Jaccard Similarity...")

    val sourceRDD = BFS.buildBfsGraph(bfsGraph.mapVertices((vertexId, vd) => {
      if (vertexId != nodeNames.get(video_id).get)
        vd
      else {
        (0, vd._2)
      }
    })).vertices.filter(vertexVal => vertexVal._2._1 != Int.MaxValue)
    sourceRDD.cache()
    bfsGraph.vertices.foreach(vertext => {
      if (vertext._2._1 != Int.MaxValue)
        println(vertext._2._1)
    })
    println(sourceRDD.count + ", bfsVertices: " + bfsGraph.vertices.count)

    val jaccard = recommendationScores.map(item => {
      val targetRDD = BFS.buildBfsGraph(bfsGraph.mapVertices((vertexId, vd) => {
        if (vertexId != item._1)
          vd
        else {
          (0, vd._2)
        }
      })).vertices.filter(vertexVal => vertexVal._2._1 != Int.MaxValue)
      if (item._2 == "http://av.tib.eu/resource/video/15727") {
        targetRDD.foreach(vertex => print(vertex._2._2 + " "))
      }
      val jaccardSimilarityA: Double = (sourceRDD.intersection(targetRDD)).count.toDouble
      val jaccardSimilarityB: Double = (sourceRDD.union(targetRDD)).count.toDouble
      println("Jaccard Similarity of " + item._2 + ": " + (jaccardSimilarityA / jaccardSimilarityB))
      jaccardSimilarityA / jaccardSimilarityB
    })
    val durchschnitt = (jaccard.aggregate(0.0)({ (sum, ch) => sum + ch }, { (p1, p2) => p1 + p2 })) / jaccard.size.toDouble
    var recommendationScoresJaccardHigh = recommendationScores.filter(score => jaccard(recommendationScores.indexOf(score)) > durchschnitt)
    var recommendationScoresJaccardLow = recommendationScores.filter(score => jaccard(recommendationScores.indexOf(score)) <= durchschnitt)

    println()
    println()
    println("Scores:")

    recommendationScoresFiltered.indices.foreach(i => { println((i + 1) + ". " + recommendationScoresFiltered(i)._2 + ", Score: " + recommendationScoresFiltered(i)._3) })
    println()
    println()
    println("Scores with Jaccard-Filtering:")

    recommendationScoresJaccardHigh.indices.foreach(i => { println((i + 1) + ". " + recommendationScoresFiltered(i)._2 + ", Score: " + recommendationScoresFiltered(i)._3) })

    println()
    println()
    println("Stärkste DBP Entitäten des Videos:")

    var dbpNodeScoresForStartVid = HashMap[String, Double]().withDefaultValue(0.0)

    recommendationScoresJaccardHigh.foreach(x => {
      println("Video " + x._2)
      x._4.map(z => {
        if (z._2.size >= 3) {

          var thirdElementName = z._2(1)._1
          var thirdElementId = nodeNames(thirdElementName)
          if (dbpIds.contains(thirdElementId)) {
            dbpNodeScoresForStartVid(thirdElementName) += z._1
          }

        }
      })

    })
    dbpNodeScoresForStartVid.toSeq.sortBy(-_._2).take(10).foreach(y => println("Score " + y + " "))

    println()
    println()
    println("Stärkste DBP Entitäten in den Pfaden:")

    recommendationScoresJaccardHigh.foreach(x => {
      var dbpNodeScores = HashMap[String, Double]().withDefaultValue(0.0)
      println("Video " + x._2)
      x._4.map(z => {
        if (z._2.size >= 3) {

          var thirdLastElementName = z._2.reverse(1)._1
          var thirdLastElementId = nodeNames(thirdLastElementName)
          //println("third last element " + thirdLastElementName + " " + thirdLastElementId)
          if (dbpIds.contains(thirdLastElementId)) {
            dbpNodeScores(thirdLastElementName) += z._1
          }

        }
      })

      dbpNodeScores.toSeq.sortBy(-_._2).take(3).foreach(y => println("Score " + y + " "))
    })

    println()
    println()
    println("Stärkste YAGO Types in den Pfaden:")

    val file = new File(OUTPUT_PATH+"out.csv")
    file.mkdirs()
    val fw = new FileWriter(file, true)
    recommendationScoresJaccardHigh.foreach(x => {
      var yagoNodeScores = HashMap[String, Double]().withDefaultValue(0.0)
      println("Video " + x._2)
      x._4.map(z => {

        if (z._2.size >= 3) {

          var yagoTypesInPath = z._2.filter(y => {
            var elementID = nodeNames(y._1)
            yagoIds.contains(elementID)
          })
          var dboTypesInPath = z._2.filter(y => {
            var elementID = nodeNames(y._1)
            dboIds.contains(elementID)
          })
          yagoTypesInPath.foreach(y => {
            yagoNodeScores(y._1) += z._1
          })
        }
      })

      val keywords = yagoNodeScores.toSeq.sortBy(-_._2).take(3)
      keywords.foreach(y => println("Score " + y + " "))

      fw.write(video_id.trim() + "," + x._2 + "," + x._3 + ",\'" + keywords.mkString(";") + "\'\n")
    })
    fw.close()
  }

  def input(sc: SparkContext): RDD[Edge[Double]] = {
    var id: Int = 0

    /*for (line <- Source.fromFile("../data/Filtered/PAGE_LINKS_sorted_count.txt")("UTF-8").getLines()) {
      val fields = line.split(" ")

      val vertexId1 = nodeNames.getOrElseUpdate(fields(0), {
        id += 1
        id - 1
      })
      val vertexId2 = nodeNames.getOrElseUpdate(fields(1), {
        id += 1
        id - 1
      })
      
      linkToIds = linkToIds + (vertexId2)
      
      typeEdges = typeEdges :+ (Edge(vertexId1, vertexId2, fields(2).toDouble / fields(3).toDouble))
      typeEdges = typeEdges :+ (Edge(vertexId2, vertexId1, fields(2).toDouble / fields(3).toDouble))
    }
    
    println("finished importing DBPedia Links")
*/
    /*
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
      dbpIds = dbpIds + (vertexId1)
      
      typeEdges = typeEdges :+ (Edge(vertexId1, vertexId2, (1.0/3.0)*1.0))
      typeEdges = typeEdges :+ (Edge(vertexId2, vertexId1, 1.0/fields(2).toDouble))
    }
    
    println("finished importing DBPedia Types")
*/
    for (line <- Source.fromFile("../data/Filtered/GND_DBPEDIA_filtered_sorted_count.txt")("UTF-8").getLines()) {
      //for (line <- Source.fromFile("../data/test1b/t1_gnd_dbp_filtered_sorted_count.txt")("UTF-8").getLines()) {
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
      //for (line <- Source.fromFile("../data/test1b/t1_tib_gnd_filtered_sorted_count_1.txt")("UTF-8").getLines()) {
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
      //for (line <- Source.fromFile("../data/test1b/t1_tib_gnd_filtered_sorted_count_1.txt")("UTF-8").getLines()) {
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

      typeEdges = typeEdges :+ (Edge(vertexId1, vertexId2, (1 / fields(2).toDouble))) //(1.0/3.0)*(1 / fields(2).toDouble)))
      typeEdges = typeEdges :+ (Edge(vertexId2, vertexId1, 1 / fields(3).toDouble))

    }

    println("finished importing YAGO Types")
    /*
    for (line <- Source.fromFile("../data/Filtered/yago_supertypes_filtered_sorted_count.txt")("UTF-8").getLines()) {
    //for (line <- Source.fromFile("../data/test1b/t1_tib_gnd_filtered_sorted_count_1.txt")("UTF-8").getLines()) {
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
    edges
  }

}
