package com.danolithe.spark

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object BFSRecommender {

  val initialMsg = (Set((1.0, List[(String, String)]())), Set[Long]())

  def buildRecommenderGraph(graph: Graph[(
        String,           //Node name 
        Set[(             //Set for paths
          Double,         //Path weight
          List[(String, String)])], //Pathlist (Name, Type)
        Set[Long],        //Set of allready reached nodes
        Boolean,          //Start node
        String),          //Node type
        Double            //Edge weight
        ]) = {
    println("build pregel...")
    graph.pregel(initialMsg, 100, EdgeDirection.Out)(this.vprog, this.sendMsg, this.mergeMsg)
  }

  def vprog(vertexId: VertexId, value: (String, Set[(Double, List[(String, String)])], Set[Long], Boolean, String), message: (Set[(Double, List[(String, String)])], Set[Long])): (String, Set[(Double, List[(String, String)])], Set[Long], Boolean, String) = {
    
    if(message == initialMsg && value._4)
      (value._1, Set((1.0,List[(String, String)]())), value._3, true, value._5)
    else if (message == initialMsg) {
      value
    }
    else
      (value._1, value._2 ++ message._1, value._3 ++ message._2, false, value._5)
  }

  def sendMsg(triplet: EdgeTriplet[(String, Set[(Double, List[(String, String)])], Set[Long], Boolean, String), Double]): Iterator[(VertexId, (Set[(Double, List[(String, String)])], Set[Long]))] = {
    val sourceVertex = triplet.srcAttr

    if(sourceVertex._4 || !(sourceVertex._3.isEmpty || sourceVertex._3.contains(triplet.dstId) || sourceVertex._5 == "VIDEO")) {
      Iterator((triplet.dstId.longValue(), (sourceVertex._2.map(triple => (triple._1 * triplet.attr.doubleValue(), triple._2  :+ (sourceVertex._1,sourceVertex._5))), sourceVertex._3 + triplet.srcId.longValue())))
    } else 
      Iterator.empty
  }

  def mergeMsg(msg1: (Set[(Double, List[(String, String)])], Set[Long]), msg2: (Set[(Double, List[(String, String)])], Set[Long])): (Set[(Double, List[(String, String)])], Set[Long]) = (msg1._1 ++ msg2._1, msg1._2 ++ msg2._2)

}