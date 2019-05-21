import java.util

import graph.Edge
import schema.ISchemaElement

class IGSIScala(silo: GraphSiloScala) extends Serializable {
  val graphSilo: GraphSiloScala = silo
//  val graphDatabase: OrientDBScala = database

  def tryAdd(schemaElements: Set[ISchemaElement]): Unit = { //TODO: payload
    val url = "remote:localhost/newtestplus"
    val username = "admin"
    val password = "admin"
    val graphDatabase: OrientDBScala = new OrientDBScala(url, username, password)

    var finalSchemaElement: ISchemaElement = null
    //update instance - schema relations, delete if necessary
    val schemaIterator: Iterator[ISchemaElement] = schemaElements.iterator

    while(schemaIterator.hasNext){
      val schemaElement = schemaIterator.next()
      //create schema element from first one
      if (finalSchemaElement == null) finalSchemaElement = schemaElement
      val schemaEdges: util.Set[(Edge, Edge)] = schemaElement.getSchemaEdges
      if (!schemaEdges.isEmpty){
        //get vertexID of all outgoing edges
        val vertexID: String = schemaEdges.iterator.next._1.start
        //check if previously known
        val optional = silo.getPreviousElementID(vertexID)
        if (optional.isDefined) { //instance (vertex) was known before
          val schemaHash: Integer = optional.get
          if (schemaHash != schemaElement.hashCode()) { //it was something else before, remove link to old schema element
            val activeLinks: Integer = silo.removeNodeFromSchemaElement(vertexID, schemaHash)
            //add link to new schema element
            silo.addNodeFromSchemaElement(vertexID, schemaElement.hashCode)
            //check if old schema element is still needed, delete otherwise from schema graph summary
            if (activeLinks <= 0) graphDatabase.deleteSchemaElement(schemaHash)
          }
        }
        //TODO: maybe move to inner loop above
        val edgeIterator: util.Iterator[(Edge, Edge)] = schemaEdges.iterator
        while (edgeIterator.hasNext){
          val edgeTuple = edgeIterator.next()
          val optional = silo.getPreviousLinkID(edgeTuple._1.hashCode)
          if (optional.isDefined) { //instance level edge was known before
            val linkHash: Integer = optional.get
            if (linkHash != edgeTuple._2.hashCode) { //it was something else before, remove link to old schema edge
              val activeLinks: Integer = silo.removeEdgeFromSchemaEdge(edgeTuple._1.hashCode, linkHash)
              //add link to new schema edge
              silo.addEdgeFromSchemaEdge(edgeTuple._1.hashCode, edgeTuple._2.hashCode)
              //check if old schema edge is still needed, delete otherwise from schema graph summary
              if (activeLinks <= 0) graphDatabase.deleteSchemaEdge(linkHash)
            }
          }
        }
      }

    }
    //TODO: there may be empty nodes without schema edges
    //if it is something new, actually write it to the schema graph summary
    if (!graphDatabase.exists(finalSchemaElement.getID)) graphDatabase.writeSchemaElementWithEdges(finalSchemaElement)

    graphDatabase.close()
  }
}
