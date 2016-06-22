package com.danolithe.spark

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object BFSRecommender {

  val initialMsg = (Set((0.0, 0, List[String](), Set[Long]())))

  def buildRecommenderGraph(graph: Graph[(String, Set[(Double, Int, List[String], Set[Long])], Boolean, Boolean), Double]) = {
    println("build pregel...")
    graph.pregel(initialMsg, 1000, EdgeDirection.Out)(this.vprog, this.sendMsg, this.mergeMsg)
  }

  def vprog(vertexId: VertexId, value: (String, Set[(Double, Int, List[String], Set[Long])], Boolean, Boolean), message: (Set[(Double, Int, List[String], Set[Long])])): (String, Set[(Double, Int, List[String], Set[Long])], Boolean, Boolean) = {
    
    if(message == initialMsg && value._3)
      (value._1, Set((0.0,0,List[String](), Set[Long]())), true, value._4)
    else if (message == initialMsg) {
      println("initial message " + vertexId + ": " + value._1 + "			value: " + value)
      value
    }
    else
      (value._1, value._2 ++ message, false, value._4)
  }

  def sendMsg(triplet: EdgeTriplet[(String, Set[(Double, Int, List[String], Set[Long])], Boolean, Boolean), Double]): Iterator[(VertexId, (Set[(Double, Int, List[String], Set[Long])]))] = {
    val sourceVertex = triplet.srcAttr

    val paths = sourceVertex._2.filter(path => (sourceVertex._3 || !(path._4.isEmpty || path._4.contains(triplet.dstId.longValue()) || sourceVertex._4)))
    if(paths.size > 0){
      println(sourceVertex._1, (triplet.dstAttr._1, (paths.map(tuple => (tuple._1 + triplet.attr.doubleValue(), tuple._2, tuple._4 + triplet.srcId.longValue())))))
      Iterator((triplet.dstId.longValue(), (paths.map(triple => (triple._1 + triplet.attr.doubleValue(), (triple._2 + 1), triple._3 :+ sourceVertex._1, triple._4 + triplet.srcId.longValue())))))
    } else
      Iterator.empty
  }

  def mergeMsg(msg1: (Set[(Double, Int, List[String], Set[Long])]), msg2: (Set[(Double, Int, List[String], Set[Long])])): (Set[(Double, Int, List[String], Set[Long])]) = (msg1 ++ msg2)

}