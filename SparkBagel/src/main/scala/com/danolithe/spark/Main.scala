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

   var nodeNames = HashMap[String, Long]()
    
    var videoIds = Set[Long]()
    var gndIds = Set[Long]()
    var dbpIds = Set[Long]()
    var yagoIds = Set[Long]()
    var dboIds = Set [Long]()
    var linkToIds = Set [Long]()
    var yovistoIds = Set[Long]()
    var subjectIds = Set[Long]()
    var superTypeLevels = HashMap[Long, Int]()
    
    
    var typeEdges = List[Edge[Double]]()
    
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
      typeEdges = typeEdges :+ (Edge(vertexId2, vertexId1, fields(2).toDouble / fields(4).toDouble))

    }
    
    println("finished importing TIB-GND")
    
    for (line <- Source.fromFile("../data/Filtered/yovistoextract_dbpedia_filtered_count.txt")("UTF-8").getLines()) {
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

      yovistoIds = yovistoIds + (vertexId1)
      dbpIds = dbpIds + (vertexId2)

      typeEdges = typeEdges :+ (Edge(vertexId1, vertexId2, 1 / fields(2).toDouble))
      typeEdges = typeEdges :+ (Edge(vertexId2, vertexId1, 1 / fields(3).toDouble))

    }
    
    println("finished importing YOVISTO-DBP")
    
    
    for (line <- Source.fromFile("../data/Filtered/merge_dbp_yago_count.txt")("UTF-8").getLines()) {
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
      superTypeLevels.getOrElseUpdate(vertexId2, 0)
      
      typeEdges = typeEdges :+ (Edge(vertexId1, vertexId2, 1.0 / fields(2).toDouble))//(1.0/3.0)*(1 / fields(2).toDouble)))
      typeEdges = typeEdges :+ (Edge(vertexId2, vertexId1, 1.0 / fields(3).toDouble))

    }

    println("finished importing YAGO Types")
    
    for (line <- Source.fromFile("../data/Filtered/yago_supertypes_filtered_sorted_count_with_hierarchy.txt")("UTF-8").getLines()) {
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

      superTypeLevels(vertexId1) = fields(4).toInt
      superTypeLevels(vertexId2) = fields(5).toInt
      
      
      
      typeEdges = typeEdges :+ (Edge(vertexId1, vertexId2, 1.0))
      
      typeEdges = typeEdges :+ (Edge(vertexId2, vertexId1, 1 / fields(3).toDouble))
      
    }

    println("finished importing YAGO super types")
    
     for (line <- Source.fromFile("../data/Filtered/en_subjects_count.txt")("UTF-8").getLines()) {
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
      subjectIds = subjectIds + (vertexId2)
      
      typeEdges = typeEdges :+ (Edge(vertexId1, vertexId2, 1.0 / fields(2).toDouble))//(1.0/3.0)*(1 / fields(2).toDouble)))
      typeEdges = typeEdges :+ (Edge(vertexId2, vertexId1, 1.0 / fields(3).toDouble))

    }

    println("finished importing DBpedia Subjects")
    
    var edges: RDD[Edge[Double]] = sc.parallelize(typeEdges)
    edges
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

    
    var edges: RDD[Edge[Double]] = input(sc)

    // RDD[(ID, (name, Set[(path_len, path_nodecount, path_nodenames, path_type)], visited, isTarget, nodeType)))]
    val nodes: RDD[(VertexId, (String, Set[(Double, List[(String, String)])], Set[Long], Boolean, String))] = sc.parallelize(nodeNames.toSeq.map (e => {
        if(videoIds.contains(e._2) || yovistoIds.contains(e._2)){
           (e._2, (e._1, Set[(Double, List[(String, String)])](), Set[Long](), e._1 == video_id, "RESOURCE")) 
        } else if (gndIds.contains(e._2)){
           (e._2, (e._1, Set[(Double, List[(String, String)])](), Set[Long](), e._1 == video_id, "GND"))
        } else if (dbpIds.contains(e._2)){
          (e._2, (e._1, Set[(Double, List[(String, String)])](), Set[Long](), e._1 == video_id, "DBPEDIA"))
        } else if (subjectIds.contains(e._2)){
          (e._2, (e._1, Set[(Double, List[(String, String)])](), Set[Long](), e._1 == video_id, "SUBJECT"))
        } else{
          (e._2, (e._1, Set[(Double, List[(String, String)])](), Set[Long](), e._1 == video_id, "YAGO"))
        }
    }))

    val graph: Graph[(
        String,           //Node name 
        Set[(             //Set for paths
          Double,         //Path weight
          List[(String, String)])], //Pathlist (Name, Type)
        Set[Long],        //Set of already reached nodes
        Boolean,          //Start node
        String),          //Node type
        Double            //Edge weight
        ] = Graph(nodes, edges)
    println("number of edges: " + graph.edges.count().toInt)
    println("number of vertices: " + graph.vertices.count().toInt)

    val resultGraph = BFSRecommender.buildRecommenderGraph(graph)
    
    println("clearing cache")
    
    graph.unpersist(blocking = false)
    println("done clearing cache")
    resultGraph.cache()
    println("done caching result graph")
    
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
    var recommendationScores: Array[(Long, String, Double, Set[(Double, List[(String, String)])])] = resultGraph.vertices.map[(Long, String, Double, Set[(Double, List[(String, String)])])](node => {
      val aggr:Double = node._2._2.foldLeft(0.0){
        (value:Double,el) => {
          val semanticCount: Double = el._2.count(_._2 == "YAGO")
          if (semanticCount >= 1)
            value + (el._1 * (1.0 + 1.0/(semanticCount)))
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
    
    var recommendationScoresYovisto: Array[(Long, String, Double, Set[(Double, List[(String, String)])])] = resultGraph.vertices.map[(Long, String, Double, Set[(Double, List[(String, String)])])](node => {
      val aggr:Double = node._2._2.foldLeft(0.0){
        (value:Double,el) => {
          val semanticCount: Double = el._2.count(_._2 == "YAGO")
          if (semanticCount >= 1)
            value + (el._1 * (1.0 + 1.0/(semanticCount)))
          else
            value + el._1
        }
      }
      if (aggr == 0) {
        (node._1, node._2._1, -0.0, node._2._2)
      } else {
        (node._1, node._2._1, aggr, node._2._2)
      }
    }).collect().filter(score => (yovistoIds.contains(score._1) && score._2 != video_id)).sortBy(-_._3).take(20);
    
    resultGraph.unpersist(blocking = false)
    
    println()
    println()
    println("Applying Jaccard Similarity...")
    
    val bfsGraph = graph.mapVertices((vertexId, vd)  => (Int.MaxValue, vd._5))
    println("Prepared bfs graph...")
    
    bfsGraph.cache()
    
    val sourceRDD = BFS.buildBfsGraph(bfsGraph.mapVertices((vertexId, vd) => {
      if(vertexId != nodeNames.get(video_id).get)
        vd
      else {
          (0, vd._2)
        }
    })).vertices.filter(vertexVal =>  vertexVal._2._1 != Int.MaxValue)
    sourceRDD.cache()
    bfsGraph.vertices.foreach(vertext => {
      if(vertext._2._1 != Int.MaxValue)
        println(vertext._2._1)
    })
    println(sourceRDD.count + ", bfsVertices: " + bfsGraph.vertices.count)
    
    
    
    val jaccard = recommendationScores.map(item => {
      val targetRDD = BFS.buildBfsGraph(bfsGraph.mapVertices((vertexId, vd) => {
        if(vertexId != item._1)
          vd
        else {
          (0, vd._2)
        }
      })).vertices.filter(vertexVal => vertexVal._2._1 != Int.MaxValue)
      if (item._2 == "http://av.tib.eu/resource/video/15727") {
        targetRDD.foreach(vertex => print(vertex._2._2 + " "))
      }
      val jaccardSimilarityA : Double = (sourceRDD.intersection(targetRDD)).count.toDouble
      val jaccardSimilarityB : Double = (sourceRDD.union(targetRDD)).count.toDouble
      println("Jaccard Similarity of " + item._2 + ": " + (jaccardSimilarityA/jaccardSimilarityB))
      jaccardSimilarityA/jaccardSimilarityB
    })
    val durchschnitt = (jaccard.aggregate(0.0)({ (sum, ch) => sum + ch }, { (p1, p2) => p1 + p2 }))/jaccard.size.toDouble
    var recommendationScoresJaccardHigh = recommendationScores.filter(score => jaccard(recommendationScores.indexOf(score)) >= durchschnitt)
    var recommendationScoresJaccardLow = recommendationScores.filter(score => jaccard(recommendationScores.indexOf(score)) < durchschnitt)
    
    
    
    
    println()
    println()
    println("Scores:")    
    
    recommendationScoresFiltered.indices.foreach(i => { println((i + 1) + ". " + recommendationScoresFiltered(i)._2 + ", Score: " + recommendationScoresFiltered(i)._3) })
    println()
    println()
    println("Scores with Jaccard-Filtering:") 
    
    recommendationScores.indices.foreach(i => { 
      if (recommendationScoresJaccardHigh.contains(recommendationScores(i)) ){
        println((i + 1) + ". " + recommendationScoresFiltered(i)._2 + ", Score: " + recommendationScoresFiltered(i)._3)
        } 
      })
    
    
    println()
    println()
    println("Stärkste DBP Entitäten des Videos:")    
    
    var dbpNodeScoresForStartVid = HashMap[String, Double]().withDefaultValue(0.0)
      
    recommendationScoresJaccardHigh.foreach( x =>  {
      println("Video " + x._2)
      x._4.map(z => {
        if(z._2.size>=3){
          
          var thirdElementName = z._2(1)._1
          var thirdElementId = nodeNames(thirdElementName)
          if (dbpIds.contains(thirdElementId)){
            dbpNodeScoresForStartVid(thirdElementName) += z._1
          }
          
        } 
      } )
      
      })
    dbpNodeScoresForStartVid.toSeq.sortBy(-_._2).take(10).foreach(y => println("Score " +y + " " ))
      
    
    println()
    println()
    println("Stärkste DBP Entitäten in den Pfaden:")    
    
    
    recommendationScoresJaccardHigh.foreach( x =>  {
      var dbpNodeScores = HashMap[String, Double]().withDefaultValue(0.0)
      println("Video " + x._2)
      x._4.map(z => {
        if(z._2.size>=3){
          
          var thirdLastElementName = z._2.reverse(1)._1
          var thirdLastElementId = nodeNames(thirdLastElementName)
          //println("third last element " + thirdLastElementName + " " + thirdLastElementId)
          if (dbpIds.contains(thirdLastElementId)){
            dbpNodeScores(thirdLastElementName) += z._1
          }
          
        } 
      } )
      
      dbpNodeScores.toSeq.sortBy(-_._2).take(3).foreach(y => println("Score " +y + " " ))
      })
      
    println()
    println()
    println("Stärkste YAGO Types in den Pfaden:")    
    
    recommendationScoresJaccardHigh.foreach( x =>  {
      var yagoNodeScores = HashMap[String, Double]().withDefaultValue(0.0)
      println("Video " + x._2)
      x._4.map(z => {
        
        if(z._2.size>=3){
          
          var yagoTypesInPath = z._2.filter(y => {
            var elementID = nodeNames(y._1)
            if (yagoIds.contains(elementID)){
              //print("Type level of " + elementID + ": "+ superTypeLevels(elementID))
              superTypeLevels(elementID)< 2
            }else {
              false
            }
          })
          var dboTypesInPath = z._2.filter(y => {
            var elementID = nodeNames(y._1)
            dboIds.contains(elementID)
          })
          yagoTypesInPath.foreach (y => {
            yagoNodeScores(y._1) += z._1
          })
        }
      })
      /*yagoNodeScores.map(score => {
         var elementID = nodeNames(score._1)
         var superTypeLevel = superTypeLevels(elementID)
         if (superTypeLevel >0){
           (score._1, score._2 * 1/superTypeLevel)
         }else {
           (score._1, score._2)
         }
      })*/
      yagoNodeScores.toSeq.sortBy(-_._2).take(3).foreach(y => println("Score " +y + " Level " + superTypeLevels(nodeNames(y._1))))
      })
      
    println()
    println()
    println("Scores Yovisto:")    
    
    recommendationScoresYovisto.indices.foreach(i => { println((i + 1) + ". " + recommendationScoresYovisto(i)._2 + ", Score: " + recommendationScoresYovisto(i)._3) })
    
    var recommendationScoresFilteredYovisto: Array[(Long, String, Double)] = recommendationScoresYovisto.map(node => (node._1, node._2, node._3));
    println()
    println()
    println("Applying Jaccard Similarity for Yovisto Scores...")
    
    val bfsGraphYovisto = graph.mapVertices((vertexId, vd)  => (Int.MaxValue, vd._5))
    println("Prepared bfs graph...")
    
    bfsGraphYovisto.cache()
    
    val sourceRDDYovisto = BFS.buildBfsGraph(bfsGraphYovisto.mapVertices((vertexId, vd) => {
      if(vertexId != nodeNames.get(video_id).get)
        vd
      else {
          (0, vd._2)
        }
    })).vertices.filter(vertexVal =>  vertexVal._2._1 != Int.MaxValue)
    sourceRDDYovisto.cache()
    bfsGraphYovisto.vertices.foreach(vertext => {
      if(vertext._2._1 != Int.MaxValue)
        println(vertext._2._1)
    })
    println(sourceRDDYovisto.count + ", bfsVertices: " + bfsGraphYovisto.vertices.count)
    
    
    
    val jaccardYovisto = recommendationScoresYovisto.map(item => {
      val targetRDD = BFS.buildBfsGraph(bfsGraphYovisto.mapVertices((vertexId, vd) => {
        if(vertexId != item._1)
          vd
        else {
          (0, vd._2)
        }
      })).vertices.filter(vertexVal => vertexVal._2._1 != Int.MaxValue)
      
      val jaccardSimilarityA : Double = (sourceRDDYovisto.intersection(targetRDD)).count.toDouble
      val jaccardSimilarityB : Double = (sourceRDDYovisto.union(targetRDD)).count.toDouble
      println("Jaccard Similarity of " + item._2 + ": " + (jaccardSimilarityA/jaccardSimilarityB))
      jaccardSimilarityA/jaccardSimilarityB
    })
    val durchschnittYovisto = (jaccardYovisto.aggregate(0.0)({ (sum, ch) => sum + ch }, { (p1, p2) => p1 + p2 }))/jaccardYovisto.size.toDouble
    var recommendationScoresJaccardHighYovisto = recommendationScoresYovisto.filter(score => jaccardYovisto(recommendationScoresYovisto.indexOf(score)) >= durchschnittYovisto)
    var recommendationScoresJaccardLowYovisto = recommendationScoresYovisto.filter(score => jaccardYovisto(recommendationScoresYovisto.indexOf(score)) < durchschnittYovisto)
    
    
    
    
   /* println()
    println()
    println("Scores:")    
    
    recommendationScoresFilteredYovisto.indices.foreach(i => { println((i + 1) + ". " + recommendationScoresFilteredYovisto(i)._2 + ", Score: " + recommendationScoresFilteredYovisto(i)._3) })*/
    println()
    println()
    println("Scores with Jaccard-Filtering:") 
    
    recommendationScoresYovisto.indices.foreach(i => { 
      if (recommendationScoresJaccardHighYovisto.contains(recommendationScoresYovisto(i)) ){
        println((i + 1) + ". " + recommendationScoresFilteredYovisto(i)._2 + ", Score: " + recommendationScoresFilteredYovisto(i)._3)
        } 
      })
    
    
    println()
    println()
    println("Stärkste DBP Entitäten des Videos:")    
    
    var dbpNodeScoresForStartVidYovisto = HashMap[String, Double]().withDefaultValue(0.0)
      
    recommendationScoresJaccardHighYovisto.foreach( x =>  {
      println("Artikel " + x._2)
      x._4.map(z => {
        if(z._2.size>=2){
          
          var thirdElementName = z._2(0)._1
          var thirdElementId = nodeNames(thirdElementName)
          if (dbpIds.contains(thirdElementId)){
            dbpNodeScoresForStartVidYovisto(thirdElementName) += z._1
          }
          
        } 
      } )
      
      })
    dbpNodeScoresForStartVidYovisto.toSeq.sortBy(-_._2).take(10).foreach(y => println("Score " +y + " " ))
      
    
    println()
    println()
    println("Stärkste DBP Entitäten in den Pfaden:")    
    
    
    recommendationScoresJaccardHighYovisto.foreach( x =>  {
      var dbpNodeScores = HashMap[String, Double]().withDefaultValue(0.0)
      println("Video " + x._2)
      x._4.map(z => {
        if(z._2.size>=2){
          
          var thirdLastElementName = z._2.reverse(0)._1
          var thirdLastElementId = nodeNames(thirdLastElementName)
          //println("third last element " + thirdLastElementName + " " + thirdLastElementId)
          if (dbpIds.contains(thirdLastElementId)){
            dbpNodeScores(thirdLastElementName) += z._1
          }
          
        } 
      } )
      
      dbpNodeScores.toSeq.sortBy(-_._2).take(3).foreach(y => println("Score " +y + " " ))
      })
      
    println()
    println()
    println("Stärkste YAGO Types in den Pfaden:")    
    
    recommendationScoresJaccardHighYovisto.foreach( x =>  {
      var yagoNodeScores = HashMap[String, Double]().withDefaultValue(0.0)
      println("Video " + x._2)
      x._4.map(z => {
        
        if(z._2.size>=3){
          
          var yagoTypesInPath = z._2.filter(y => {
            var elementID = nodeNames(y._1)
            if (yagoIds.contains(elementID)){
              //print("Type level of " + elementID + ": "+ superTypeLevels(elementID))
              superTypeLevels(elementID)< 2
            }else {
              false
            }
          })
          var dboTypesInPath = z._2.filter(y => {
            var elementID = nodeNames(y._1)
            dboIds.contains(elementID)
          })
          yagoTypesInPath.foreach (y => {
            yagoNodeScores(y._1) += z._1
          })
        }
      })
      /*yagoNodeScores.map(score => {
         var elementID = nodeNames(score._1)
         var superTypeLevel = superTypeLevels(elementID)
         if (superTypeLevel >0){
           (score._1, score._2 * 1/superTypeLevel)
         }else {
           (score._1, score._2)
         }
      })*/
      yagoNodeScores.toSeq.sortBy(-_._2).take(3).foreach(y => println("Score " +y + " Level " + superTypeLevels(nodeNames(y._1))))
      })
      println("YAGO IDS SIZE: " + yagoIds.size)
      println("DBP IDS SIZE: " + dbpIds.size)
      println("YOVISTO IDS SIZE: " + yovistoIds.size)
      
      
    

//    resultGraph.vertices.saveAsTextFile(OUTPUT_PATH + "vertices.txt")
//    resultGraph.edges.saveAsTextFile(OUTPUT_PATH + "edges.txt")

    //    val result = graph.pregel(BFSRecommender.initialMsg, 10, EdgeDirection.Out)(BFSRecommender.vprog, BFSRecommender.sendMsg, BFSRecommender.mergeMsg)
    //    result.vertices.foreach(println)
    //result.vertices.foreach(println)
    //    result.vertices.saveAsTextFile(OUTPUT_PATH + "vertices")
    //    result.edges.saveAsTextFile(OUTPUT_PATH + "edges")

  }
}
