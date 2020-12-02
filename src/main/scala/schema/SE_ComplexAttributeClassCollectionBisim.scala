package schema

import org.apache.spark.graphx.{EdgeContext, EdgeTriplet}

import scala.collection.mutable

object SE_ComplexAttributeClassCollectionBisim {

  def sendMessage(triplet: EdgeTriplet[SchemaElement, (String, String, String, String)]): Unit = {
    // Send message to destination vertex containing types and property


    //add neighbor element connected over this property
    triplet.srcAttr.neighbors.put(triplet.attr._2, triplet.dstAttr)
    //add datasource/source _graph as payload
    triplet.srcAttr.payload.add(triplet.attr._4)
    //add src vertex as instance
    triplet.srcAttr.instances.add(triplet.attr._1)
//    val srcSet = new mutable.HashSet[SchemaElement]()
//    srcSet.add(triplet.srcAttr._2)
    //triplet.sendToSrc((triplet.srcAttr._2.getID(), srcSet))
    Iterator((triplet.dstId, triplet.srcAttr))
    /**
     * returns tuple of id + set with only one schema element
     */
  }

  def mergeMessage(a: (Int, mutable.HashSet[SchemaElement]), b: (Int, mutable.HashSet[SchemaElement])): (Int, mutable.HashSet[SchemaElement]) = {
    val mergedElements = new mutable.HashSet[SchemaElement]()
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
