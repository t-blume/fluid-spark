package schema

import org.apache.spark.graphx.EdgeContext

import scala.collection.mutable

trait SchemaExtraction extends Serializable {

  def sendMessage(triplet: EdgeContext[Set[(String, String)], (String, String, String, String), (Int, mutable.HashSet[VertexSummary])]): Unit

  def mergeMessage(a: (Int, mutable.HashSet[VertexSummary]), b: (Int, mutable.HashSet[VertexSummary])): (Int, mutable.HashSet[VertexSummary])

}
