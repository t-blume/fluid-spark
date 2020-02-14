package input

import database.Constants
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

object RDFGraphParser {
  val allowLiteralsAsSubject = false


  var useIncoming = false
  var classSignal = "By default, we do not use types. Sincerely, 55A01060C1F1D3B9F5CCD5C2A1CDFB1C45332B3CD83F0CD7FA2034B80AE3EAAB"

  def parse(triples: RDD[Edge[(String, String, String, String)]]): Graph[Set[(String, String)], (String, String, String, String)] = {

    // (label, source)
    val vertices: RDD[(VertexId, Set[(String, String)])] = triples.flatMap {
      case edge: Edge[(String, String, String, String)] =>
        if (edge.attr != null && (edge.attr._2.toString == classSignal || classSignal == Constants.ALL_LABEL))
          Set((edge.srcId, Set((edge.attr._3, edge.attr._4))))
        else
          Set[(VertexId, Set[(String, String)])]()

    }.reduceByKey(_ ++ _)

    val edges: RDD[Edge[(String, String, String, String)]] = triples.flatMap {
      case edge: Edge[(String, String, String, String)] =>
        if (edge.attr != null && edge.attr._2.toString != classSignal) {
          if (useIncoming) {
            // (start, label, end, defaultSource)
            if (allowLiteralsAsSubject || !edge.attr._3.startsWith("\"")) {
              val iAttr = (edge.attr._3, edge.attr._2, edge.attr._1, edge.attr._4)
              val iEdge: Edge[(String, String, String, String)] = new Edge[(String, String, String, String)](edge.dstId, edge.srcId, iAttr)
              Set(edge, iEdge)
            } else
              Set(edge)
          }
          else
            Set(edge)
        }
        else
          Set()
    }

    Graph(vertices, edges)
  }

}
