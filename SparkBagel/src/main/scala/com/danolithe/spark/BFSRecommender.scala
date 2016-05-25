package com.danolithe.spark

import org.apache.spark.graphx.Graph

object BFSRecommender {
  
  def buildRecommenderGraph(graph:Graph[String, Long]):Graph[String, Long] = {
      graph
  }
  
}