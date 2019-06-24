package schema

import classes.SchemaElement
import org.apache.spark.graphx.EdgeContext

trait SchemaExtraction extends Serializable {

  def sendMessage (triplet: EdgeContext[Set[(String, String)], (String, String, String, String), SchemaElement]): Unit
  def mergeMessage(a: SchemaElement, b: SchemaElement) : SchemaElement

}
