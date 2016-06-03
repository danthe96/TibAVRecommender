package com.danolithe.spark

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object BFSRecommender {

  val initialMsg = List((0.0, 0))

  def buildRecommenderGraph(graph: Graph[(String, List[(Double, Int)], List[Int]), Double]) = {
    graph.pregel(initialMsg, 1000, EdgeDirection.Out)(this.vprog, this.sendMsg, this.mergeMsg)
  }

  def vprog(vertexId: VertexId, value: (String, List[(Double, Int)], List[Int]), message: List[(Double, Int)]): (String, List[(Double, Int)], List[Int]) = {
    if (message == initialMsg)
      value
    else
      (value._1, value._2 ++ message, value._3 :+ vertexId.intValue())
  }

  def sendMsg(triplet: EdgeTriplet[(String, List[(Double, Int)], List[Int]), Double]): Iterator[(VertexId, List[(Double, Int)])] = {
    val sourceVertex = triplet.srcAttr

    if (sourceVertex._3.contains(triplet.dstId))
      Iterator.empty
    else
      Iterator((triplet.dstId.longValue(), sourceVertex._2.map(tuple => (tuple._1 + triplet.attr.doubleValue(), tuple._2 + 1))))
  }

  def mergeMsg(msg1: List[(Double, Int)], msg2: List[(Double, Int)]): List[(Double, Int)] = msg1 ++ msg2

}