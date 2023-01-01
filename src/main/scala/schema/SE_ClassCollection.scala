package schema

import org.apache.spark.graphx.EdgeContext

import scala.collection.mutable

object SE_ClassCollection extends SchemaExtraction {

  override def sendMessage(triplet: EdgeContext[Set[(String, String)], (String, String, String, String), (Int, mutable.HashSet[VertexSummary])]): Unit = {
    // Send message to destination vertex containing types and property
    val srcElement = new VertexSummary

    //get origin types
    if (triplet.srcAttr != null)
      for ((a, _) <- triplet.srcAttr)
        srcElement.label.add(a)

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
