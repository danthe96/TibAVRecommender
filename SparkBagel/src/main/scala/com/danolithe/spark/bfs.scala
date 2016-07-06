package com.danolithe.spark

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object BFS {

  def buildBfsGraph(graph: Graph[Double, Double]) = {
    graph.pregel(Double.PositiveInfinity, 1000, EdgeDirection.Either)(this.vprog, this.sendMsg, this.mergeMsg)
  }

  def vprog(vertexId: VertexId, value: Double, message: Double): Double = math.min(value, message)

  def sendMsg(triplet: EdgeTriplet[Double, Double]): Iterator[(VertexId, Double)] = {
    var iter:Iterator[(VertexId, Double)] = Iterator.empty
		val isSrcMarked = triplet.srcAttr != Double.PositiveInfinity
		val isDstMarked = triplet.dstAttr != Double.PositiveInfinity
		if((!isSrcMarked && isDstMarked) || (isSrcMarked && !isDstMarked)){
		   	if(isSrcMarked){
				iter = Iterator((triplet.dstId,triplet.srcAttr+1))
	  		}else{
				iter = Iterator((triplet.srcId,triplet.dstAttr+1))
	   		}
		}
		iter
  }

  def mergeMsg(msg1: Double, msg2: Double): Double = math.min(msg1, msg2)

}