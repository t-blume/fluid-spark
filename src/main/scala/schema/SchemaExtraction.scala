package schema

import org.apache.spark.graphx.EdgeContext

import scala.collection.mutable

trait SchemaExtraction extends Serializable {

  def sendMessage(triplet: EdgeContext[Set[(String, String)], (String, String, String, String), (Int, mutable.HashSet[SchemaElement])]): Unit

  def mergeMessage(a: (Int, mutable.HashSet[SchemaElement]), b: (Int, mutable.HashSet[SchemaElement])): (Int, mutable.HashSet[SchemaElement])

}
