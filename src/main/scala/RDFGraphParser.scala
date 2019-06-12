import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.util.hashing.MurmurHash3

object RDFGraphParser {
  def hash(s: String) = MurmurHash3.stringHash(s.toString).toLong


  def parse(triples: RDD[Edge[(String, String, String, String)]]): Graph[Set[(String, String)], (String, String, String, String)] = {
   val vertices: RDD[(VertexId, Set[(String, String)])] = triples.flatMap {
      case (edge: Edge[(String, String, String, String)]) =>
        if(edge.attr._2.toString == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
          Set((edge.srcId, Set((edge.attr._3, edge.attr._4))))
        else
          Set[(VertexId, Set[(String, String)])]()
    }.reduceByKey(_++_)

    val edges: RDD[Edge[(String, String, String, String)]] = triples.flatMap {
      case (edge: Edge[(String, String, String, String)]) =>
        if(edge.attr._2.toString != "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
          Set(edge)
        else
          Set()
    }
    Graph(vertices, edges)
  }
}
