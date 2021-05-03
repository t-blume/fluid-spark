package schema

import org.apache.spark.graphx.{EdgeTriplet, VertexId}

object SE_ComplexAttributeClassCollectionBisim {
  //  def sendMessage(triplet: EdgeTriplet[VertexSummary, (String, String, String, String)]): Iterator[(VertexId, VertexSummary)] = {
  //    // Send message to destination vertex containing types and property
  //    //add neighbor element connected over this property
  //    if (triplet.dstAttr.neighbors.containsKey(triplet.attr._2)){
  //      //we already had some info about this property
  //      if (triplet.dstAttr.neighbors.get(triplet.attr._2) != triplet.srcAttr){
  //        //we have new info, update it
  //        triplet.dstAttr.neighbors.put(triplet.attr._2, triplet.srcAttr)
  //        System.out.println("updating neighbor for " + triplet.dstId)
  //        return Iterator((triplet.dstId, triplet.dstAttr))
  //      }else{
  //        System.out.println("old and new are the same")
  //        System.out.println(triplet.dstAttr.neighbors.get(triplet.attr._2))
  //        System.out.println(triplet.srcAttr)
  //      }
  //    }else{
  //      // no neighbor at all
  //      System.out.println("creating neighbor for " + triplet.dstId + "(" + triplet.attr._2 + ")")
  //
  //      triplet.dstAttr.neighbors.put(triplet.attr._2, triplet.srcAttr)
  //      return Iterator((triplet.dstId, triplet.dstAttr))
  //    }
  //    System.out.println("nothing for " + triplet.srcId)
  //
  //    return Iterator.empty
  //
  //    //add datasource/source _graph as payload
  //
  //    //    triplet.srcAttr.payload.add(triplet.attr._4)
  //    //    //add src vertex as instance
  //    //    triplet.srcAttr.instances.add(triplet.attr._1)
  //    //    val srcSet = new mutable.HashSet[SchemaElement]()
  //    //    srcSet.add(triplet.srcAttr._2)
  //    //    //triplet.sendToSrc((triplet.srcAttr._2.getID(), srcSet))
  //    //    return Iterator((triplet.srcId, triplet.srcAttr))
  //  }

  def sendMessage(triplet: EdgeTriplet[VertexSummaryOLD, (String, String, String, String)]): Iterator[(VertexId, VertexSummaryOLD)] = {
    // Send message to destination vertex containing types and property
    //add neighbor element connected over this property
    if (triplet.srcAttr.neighbors.containsKey(triplet.attr._2)) {
      //we already had some info about this property
      if (triplet.srcAttr.neighbors.get(triplet.attr._2).getID() != triplet.dstAttr.getID()) {
        //we have new info, update it
        triplet.srcAttr.neighbors.put(triplet.attr._2, triplet.dstAttr)
        System.out.println("updating neighbor for " + triplet.srcId)
        return Iterator((triplet.srcId, triplet.srcAttr))
      } else {
        System.out.println("old and new are the same")
        System.out.println(triplet.srcAttr.neighbors.get(triplet.attr._2))
        System.out.println(triplet.dstAttr)
      }
    } else {
      // no neighbor at all
      triplet.srcAttr.neighbors.put(triplet.attr._2, triplet.dstAttr)
      System.out.println(triplet.srcAttr)
      return Iterator((triplet.srcId, triplet.srcAttr))
    }
    System.out.println("nothing for " + triplet.srcId)

    return Iterator.empty

    //add datasource/source _graph as payload

    //    triplet.srcAttr.payload.add(triplet.attr._4)
    //    //add src vertex as instance
    //    triplet.srcAttr.instances.add(triplet.attr._1)
    //    val srcSet = new mutable.HashSet[SchemaElement]()
    //    srcSet.add(triplet.srcAttr._2)
    //    //triplet.sendToSrc((triplet.srcAttr._2.getID(), srcSet))
    //    return Iterator((triplet.srcId, triplet.srcAttr))
  }


  def sendMessage2(triplet: EdgeTriplet[VertexSummaryOLD, (String, String, String, String)]): Iterator[(VertexId, java.util.HashMap[String, VertexSummaryOLD])] = {
    val thisMap = new java.util.HashMap[String, VertexSummaryOLD]()
    thisMap.put(triplet.attr._2, triplet.dstAttr)
    Iterator((triplet.srcId, thisMap))
  }


  def mergeMessage(a: java.util.HashMap[String, VertexSummaryOLD], b: java.util.HashMap[String, VertexSummaryOLD]): java.util.HashMap[String, VertexSummaryOLD] = {
    b.forEach((p, e) => {
      a.merge(p, e, static_merge)
    })
    a
  }

  def vertex_program(a: java.util.HashMap[String, VertexSummaryOLD], b: java.util.HashMap[String, VertexSummaryOLD]): java.util.HashMap[String, VertexSummaryOLD] = {
    /*
      a should contain only one element afterwards, b should be neighbors
     */
    if (a.size() != 1){
      System.out.println("Unexpected size of left vertex summary for vertex program")
    }
    a.forEach((myself, primaryVS) => {
      System.out.println("myself: " + myself)
      // should be only a.get("_self")
      b.forEach((predicate, secondaryVS) => {
        if (predicate != null) {
          primaryVS.neighbors.put(predicate, secondaryVS)
          System.out.println("put " + predicate + " -> " + secondaryVS)
        }
      })
    })
    System.out.println("My updated self: " + a.get("_self"))
    a
  }

  def static_merge(a: VertexSummaryOLD, b: VertexSummaryOLD): VertexSummaryOLD = {
    a.merge(b)
    a
  }

  def neighbor_update(a: VertexSummaryOLD, b: VertexSummaryOLD): VertexSummaryOLD = {
    b.neighbors.forEach((p, e) => {
      a.neighbors.merge(p, e, static_merge)
    })
    a
  }

}
