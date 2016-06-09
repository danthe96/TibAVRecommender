package com.danolithe.spark

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object BFSRecommender {

  val initialMsg = (List((0.0, 0)), Set[Long]())

  def buildRecommenderGraph(graph: Graph[(String, List[(Double, Int)], Set[Long]), Double]) = {
    graph.pregel(initialMsg, 1000, EdgeDirection.Out)(this.vprog, this.sendMsg, this.mergeMsg)
  }

  def vprog(vertexId: VertexId, value: (String, List[(Double, Int)], Set[Long]), message: (List[(Double, Int)], Set[Long])): (String, List[(Double, Int)], Set[Long]) = {
    if (message == initialMsg)
      value
    else
      (value._1, value._2 ++ message._1, value._3 ++ message._2)
  }

  def sendMsg(triplet: EdgeTriplet[(String, List[(Double, Int)], Set[Long]), Double]): Iterator[(VertexId, (List[(Double, Int)], Set[Long]))] = {
    val sourceVertex = triplet.srcAttr

    if (sourceVertex._3.contains(triplet.dstId))
      Iterator.empty
    else
      Iterator((triplet.dstId.longValue(), (sourceVertex._2.map(tuple => (tuple._1 + triplet.attr.doubleValue(), tuple._2 + 1)), sourceVertex._3 + triplet.srcId.longValue())))
  }

  def mergeMsg(msg1: (List[(Double, Int)], Set[Long]), msg2: (List[(Double, Int)], Set[Long])): (List[(Double, Int)], Set[Long]) = (msg1._1 ++ msg2._1, msg1._2 ++ msg2._2)

}