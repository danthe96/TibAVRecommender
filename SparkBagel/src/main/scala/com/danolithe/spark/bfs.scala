package com.danolithe.spark

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object BFS {

  def buildBfsGraph(graph: Graph[(Int, String), Double]) = {
    graph.pregel(Int.MaxValue, 100, EdgeDirection.Out)(this.vprog, this.sendMsg, this.mergeMsg)
  }

  def vprog(vertexId: VertexId, value: (Int, String), message: Int): (Int, String) = (math.min(value._1, message), value._2)

  def sendMsg(triplet: EdgeTriplet[(Int, String), Double   ]): Iterator[(VertexId, Int)] = {
		val isSrcMarked = triplet.srcAttr._1 != Int.MaxValue
		val isDstMarked = triplet.dstAttr._1 != Int.MaxValue
		if((!isSrcMarked && isDstMarked) || (isSrcMarked && !isDstMarked)){
		   	if(isSrcMarked && ((triplet.dstAttr._2 == "DBPEDIA" && triplet.srcAttr._2 != "YAGO")|| (triplet.dstAttr._2 == "YAGO" && triplet.srcAttr._2 == "DBPEDIA") || (triplet.srcAttr._2 == "VIDEO" && triplet.dstAttr._2 == "GND")))
				  Iterator((triplet.dstId,triplet.srcAttr._1+1))
	  		else if(isDstMarked && ((triplet.srcAttr._2 == "DBPEDIA" && triplet.dstAttr._2 != "YAGO") || (triplet.dstAttr._2 == "DBPEDIA" && triplet.srcAttr._2 == "YAGO") || (triplet.dstAttr._2 == "VIDEO" && triplet.srcAttr._2 == "GND")))
				  Iterator((triplet.srcId,triplet.dstAttr._1+1))
	   	  else
          Iterator.empty	   		   
		}else
      Iterator.empty
  }

  def mergeMsg(msg1: Int, msg2: Int): Int = math.min(msg1, msg2)

}