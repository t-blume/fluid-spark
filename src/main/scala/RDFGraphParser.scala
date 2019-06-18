import database.Constants
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

object RDFGraphParser {

  def parse(triples: RDD[Edge[(String, String, String, String)]]): Graph[Set[(String, String)], (String, String, String, String)] = {
   val vertices: RDD[(VertexId, Set[(String, String)])] = triples.flatMap {
      case (edge: Edge[(String, String, String, String)]) =>
        if(edge.attr._2.toString == Constants.TYPE)
          Set((edge.srcId, Set((edge.attr._3, edge.attr._4))))
        else
          Set[(VertexId, Set[(String, String)])]()
    }.reduceByKey(_++_)

    val edges: RDD[Edge[(String, String, String, String)]] = triples.flatMap {
      case (edge: Edge[(String, String, String, String)]) =>
        if(edge.attr._2.toString != Constants.TYPE)
          Set(edge)
        else
          Set()
    }
    Graph(vertices, edges)
  }
}
