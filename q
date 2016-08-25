[1mdiff --git a/SparkBagel/pom.xml b/SparkBagel/pom.xml[m
[1mindex 12fdf16..78f8602 100644[m
[1m--- a/SparkBagel/pom.xml[m
[1m+++ b/SparkBagel/pom.xml[m
[36m@@ -3,7 +3,7 @@[m
 	<modelVersion>4.0.0</modelVersion>[m
 	<groupId>knowmin-tibav</groupId>[m
 	<artifactId>knowmin-tibav</artifactId>[m
[31m-	<version>0.6.3</version>[m
[32m+[m	[32m<version>0.6.5</version>[m
 	<name>${project.artifactId}</name>[m
 	<inceptionYear>2010</inceptionYear>[m
 [m
[1mdiff --git a/SparkBagel/src/main/scala/com/danolithe/spark/BFSRecommender.scala b/SparkBagel/src/main/scala/com/danolithe/spark/BFSRecommender.scala[m
[1mindex 763e6f4..c85bede 100644[m
[1m--- a/SparkBagel/src/main/scala/com/danolithe/spark/BFSRecommender.scala[m
[1m+++ b/SparkBagel/src/main/scala/com/danolithe/spark/BFSRecommender.scala[m
[36m@@ -3,7 +3,8 @@[m [mpackage com.danolithe.spark[m
 import org.apache.spark._[m
 import org.apache.spark.graphx._[m
 import org.apache.spark.rdd.RDD[m
[31m-[m
[32m+[m[32m//import scala.collection.mutable.Set[m
[32m+[m[32m//import scala.collection.mutable.MutableList[m
 object BFSRecommender {[m
 [m
   val initialMsg = (Set((1.0, List[(String, String)]())), Set[Long]())[m
[1mdiff --git a/SparkBagel/src/main/scala/com/danolithe/spark/Main.scala b/SparkBagel/src/main/scala/com/danolithe/spark/Main.scala[m
[1mindex cd26ede..3f7090a 100644[m
[1m--- a/SparkBagel/src/main/scala/com/danolithe/spark/Main.scala[m
[1m+++ b/SparkBagel/src/main/scala/com/danolithe/spark/Main.scala[m
[36m@@ -19,6 +19,9 @@[m [mimport java.io.File[m
 import java.io.FileReader[m
 import scala.io.Source[m
 import scala.collection.mutable.HashMap[m
[32m+[m[32m//import scala.collection.mutable.Set[m
[32m+[m[32m//import scala.collection.mutable.MutableList[m
[32m+[m
 [m
 object NodeType extends Enumeration {[m
     type NodeType = Value[m
[36m@@ -313,6 +316,8 @@[m [mobject Main {[m
     [m
     resultGraph.unpersist(blocking = false)[m
     [m
[32m+[m[41m   [m
[32m+[m[41m    [m
     println()[m
     println()[m
     println("Applying Jaccard Similarity...")[m
[36m@@ -343,6 +348,7 @@[m [mobject Main {[m
         if(vertexId != item._1)[m
           vd[m
         else {[m
[32m+[m[32m          println("Start VID")[m
           (0, vd._2)[m
         }[m
       })).vertices.filter(vertexVal => vertexVal._2._1 != Int.MaxValue)[m
[36m@@ -352,6 +358,9 @@[m [mobject Main {[m
       val jaccardSimilarityA : Double = (sourceRDD.intersection(targetRDD)).count.toDouble[m
       val jaccardSimilarityB : Double = (sourceRDD.union(targetRDD)).count.toDouble[m
       println("Jaccard Similarity of " + item._2 + ": " + (jaccardSimilarityA/jaccardSimilarityB))[m
[32m+[m[32m      println("Jacc Sim A: " + jaccardSimilarityA)[m
[32m+[m[32m      println("Jacc Sim B: " + jaccardSimilarityB)[m
[32m+[m[41m      [m
       jaccardSimilarityA/jaccardSimilarityB[m
     })[m
     val durchschnitt = (jaccard.aggregate(0.0)({ (sum, ch) => sum + ch }, { (p1, p2) => p1 + p2 }))/jaccard.size.toDouble[m
[36m@@ -465,7 +474,8 @@[m [mobject Main {[m
       })*/[m
       yagoNodeScores.toSeq.sortBy(-_._2).take(3).foreach(y => println("Score " +y + " Level " + superTypeLevels(nodeNames(y._1))))[m
       })[m
[31m-      [m
[32m+[m[41m     [m
[32m+[m[41m          [m
     println()[m
     println()[m
     println("Scores Yovisto:")    [m
[36m@@ -503,13 +513,18 @@[m [mobject Main {[m
         if(vertexId != item._1)[m
           vd[m
         else {[m
[32m+[m[32m          println("Start VID")[m
           (0, vd._2)[m
         }[m
       })).vertices.filter(vertexVal => vertexVal._2._1 != Int.MaxValue)[m
       [m
       val jaccardSimilarityA : Double = (sourceRDDYovisto.intersection(targetRDD)).count.toDouble[m
       val jaccardSimilarityB : Double = (sourceRDDYovisto.union(targetRDD)).count.toDouble[m
[32m+[m[41m      [m
       println("Jaccard Similarity of " + item._2 + ": " + (jaccardSimilarityA/jaccardSimilarityB))[m
[32m+[m[32m      println("Jacc Sim A: " + jaccardSimilarityA)[m
[32m+[m[32m      println("Jacc Sim B: " + jaccardSimilarityB)[m
[32m+[m[41m      [m
       jaccardSimilarityA/jaccardSimilarityB[m
     })[m
     val durchschnittYovisto = (jaccardYovisto.aggregate(0.0)({ (sum, ch) => sum + ch }, { (p1, p2) => p1 + p2 }))/jaccardYovisto.size.toDouble[m
[36m@@ -627,7 +642,6 @@[m [mobject Main {[m
       println("DBP IDS SIZE: " + dbpIds.size)[m
       println("YOVISTO IDS SIZE: " + yovistoIds.size)[m
       [m
[31m-      [m
     [m
 [m
 //    resultGraph.vertices.saveAsTextFile(OUTPUT_PATH + "vertices.txt")[m
[1mdiff --git a/SparkBagel/src/main/scala/com/danolithe/spark/bfs.scala b/SparkBagel/src/main/scala/com/danolithe/spark/bfs.scala[m
[1mindex 37fb427..5570a53 100644[m
[1m--- a/SparkBagel/src/main/scala/com/danolithe/spark/bfs.scala[m
[1m+++ b/SparkBagel/src/main/scala/com/danolithe/spark/bfs.scala[m
[36m@@ -3,7 +3,8 @@[m [mpackage com.danolithe.spark[m
 import org.apache.spark._[m
 import org.apache.spark.graphx._[m
 import org.apache.spark.rdd.RDD[m
[31m-[m
[32m+[m[32mimport scala.collection.mutable.Set[m
[32m+[m[32m//import scala.collection.mutable.MutableList[m
 object BFS {[m
 [m
   def buildBfsGraph(graph: Graph[(Int, String), Double]) = {[m
