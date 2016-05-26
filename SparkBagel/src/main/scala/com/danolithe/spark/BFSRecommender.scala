package com.danolithe.spark

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object BFSRecommender {
  
  def buildRecommenderGraph(graph:Graph[String, Long]):Graph[String, Long] = {
      graph
  }
  
  val initialMsg = (-1.0, 0)

def vprog(vertexId: VertexId, value: (String, Double, Int, Int), message: (Double, Int)): (String, Double, Int, Int) = {
  if (message == initialMsg)
    value
  else
    (value._1, (value._2 + message._1), message._2, value._4 min message._2 )
}

def sendMsg(triplet: EdgeTriplet[(String, Double, Int, Int), Double]): Iterator[(VertexId, (Double, Int))] = {
  val sourceVertex = triplet.srcAttr

  if (sourceVertex._3 != sourceVertex._4)
    Iterator.empty
  else
    Iterator((triplet.dstId, (((sourceVertex._2*sourceVertex._3)+triplet.attr)/(sourceVertex._3+1), sourceVertex._3+1)))
}

def mergeMsg(msg1: (Double, Int), msg2: (Double, Int)): (Double, Int) = (msg1._1+msg2._1, msg1._2)
  
}