package schema

import org.apache.spark.graphx.{EdgeContext, EdgeTriplet, VertexId}

import scala.collection.mutable

object SE_ComplexAttributeClassCollectionBisim {

  def sendMessage(triplet: EdgeTriplet[SchemaElement, (String, String, String, String)]): Iterator[(VertexId, SchemaElement)] = {
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
    Iterator((triplet.srcId, triplet.srcAttr))
  }
}
