package schema

import org.apache.spark.graphx.EdgeContext

import scala.collection.mutable

object SE_SchemEX extends SchemaExtraction {

  def sendMessage(triplet: EdgeContext[Set[(String, String)], (String, String, String, String), (Int, mutable.HashSet[SchemaElement])]): Unit = {
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
    //add datasource/source _graph as payload
    srcElement.payload.add(triplet.attr._4)
    //add src vertex as instance
    srcElement.instances.add(triplet.attr._1)
    val srcSet = new mutable.HashSet[SchemaElement]()
    srcSet.add(srcElement)
    triplet.sendToSrc((srcElement.getID(), srcSet))
  }

  def mergeMessage(a: (Int, mutable.HashSet[SchemaElement]), b: (Int, mutable.HashSet[SchemaElement])): (Int, mutable.HashSet[SchemaElement]) = {
    a._2.foreach(aElem => b._2.foreach(bElem => aElem.merge(bElem)))
    return (a._2.iterator.next().getID(), a._2)
  }
}
