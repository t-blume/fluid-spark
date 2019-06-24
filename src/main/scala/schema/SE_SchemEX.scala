package schema

import classes.SchemaElement
import org.apache.spark.graphx.EdgeContext

object SE_SchemEX extends SchemaExtraction{

  def sendMessage (triplet: EdgeContext[Set[(String, String)], (String, String, String, String), SchemaElement]): Unit = {
    // Send message to destination vertex containing types and property
    val srcElement = new SchemaElement
    val dstElement = new SchemaElement

    //get origin types
    if (triplet.srcAttr != null)
      for ((a, _) <- triplet.srcAttr)
        srcElement.label.add(a)

    //get dst types
    if (triplet.dstAttr != null)
      for ((a, _) <- triplet.dstAttr)
        dstElement.label.add(a)

    //add neighbor element connected over this property
    srcElement.neighbors.put(triplet.attr._2, dstElement)
    //add datasource/source graph as payload
    srcElement.payload.add(triplet.attr._4)
    //add src vertex as instance
    srcElement.instances.add(triplet.attr._1)
    triplet.sendToSrc(srcElement)
  }

  def mergeMessage(a: SchemaElement, b: SchemaElement) : SchemaElement  = {
      a.merge(b)
      return a
  }
}
