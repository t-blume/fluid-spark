package schema

import org.apache.spark.graphx.EdgeContext

import scala.collection.mutable

object SE_ComplexAttributeClassCollection extends SchemaExtraction {

  override def sendMessage(triplet: EdgeContext[Set[(String, String)], (String, String, String, String), (Int, mutable.HashSet[VertexSummary])]): Unit = {
    // Send message to destination vertex containing types and property
    // TODO: for k-bisimulation, re-use schema elements
    val srcElement = new VertexSummary
    val dstElement = new VertexSummary

    //get origin types
    if (triplet.srcAttr != null)
      for ((a, _) <- triplet.srcAttr)
        srcElement.label += a

    //get dst types
    if (triplet.dstAttr != null)
      for ((a, _) <- triplet.dstAttr)
        dstElement.label += a

    //add neighbor element connected over this property
    srcElement.neighbors + (triplet.attr._2 -> dstElement)
    //add datasource/source _graph as payload
    srcElement.payload.add(triplet.attr._4)
    //add src vertex as instance
    srcElement.instances.add(triplet.attr._1)
    val srcSet = new mutable.HashSet[VertexSummary]()
    srcSet.add(srcElement)
    triplet.sendToSrc((srcElement.getID(), srcSet))

    /**
     * returns tuple of id + set with only one schema element
     */
  }

  override def mergeMessage(a: (Int, mutable.HashSet[VertexSummary]), b: (Int, mutable.HashSet[VertexSummary])): (Int, mutable.HashSet[VertexSummary]) = {
    val mergedElements = new mutable.HashSet[VertexSummary]()
    val aIter = a._2.iterator
    while (aIter.hasNext){
      val bIter = b._2.iterator
      val aElem =  aIter.next()

      while (bIter.hasNext){
        val bElem = bIter.next()
        aElem.merge(bElem)
      }
      mergedElements.add(aElem)
    }
    (mergedElements.iterator.next().getID(), mergedElements)
  }

}
