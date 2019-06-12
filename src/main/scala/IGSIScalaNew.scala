import java.util

import database.OrientDb
import graph.Edge
import schema.{ChangeTracker, ISchemaElement}

import scala.collection.mutable

class IGSIScalaNew() extends Serializable {


  def tryAdd(schemaElements: mutable.HashSet[ISchemaElement]): Unit = { //TODO: payload
    //    val graphDatabase: OrientDBScala = new OrientDBScala(url, username, password)
    val graphDatabase: OrientDb = OrientDb.getInstance()

    var added = false
    //update instance - schema relations, delete if necessary
    val schemaIterator: Iterator[ISchemaElement] = schemaElements.iterator
    while (schemaIterator.hasNext) {
      val schemaElement = schemaIterator.next()
      //create schema element from first one
      if (!added) {
        //TODO: there may be empty nodes without schema edges when using chaining
        //if it is something new, actually write it to the schema graph summary
        if (!graphDatabase.exists(schemaElement.getID)) {
          graphDatabase.writeSchemaElementWithEdges(schemaElement)
          ChangeTracker.getSchemaElementsAddedThisIteration.add(schemaElement.getID)
        }
        added = true
        // add schema hash and the number of schema elements in set (corresponds to the number of instances summarized)
        ChangeTracker.getSchemaElementsThisIteration.merge(schemaElement.getID, schemaElements.size, (o, n) => o + n)
      }
      val schemaEdges: util.Set[(Edge, Edge)] = schemaElement.getSchemaEdges
      if (!schemaEdges.isEmpty) {
        //get vertexID of all outgoing edges
        val vertexID: String = schemaEdges.iterator.next._1.start
        //check if previously known
        val schemaID = graphDatabase.getPreviousElementID(vertexID.hashCode)
        if (schemaID != null) { //instance (vertex) was known before
          val schemaHash: Integer = schemaID
          if (schemaHash != schemaElement.getID()) {
            //CASE: instance was known but with a different schema
            // it was something else before, remove link to old schema element
            ChangeTracker.getInstancesChangedThisIteration.add(vertexID.hashCode)
            val activeLinks: Integer = graphDatabase.removeNodeFromSchemaElement(vertexID.hashCode, schemaHash)
            ChangeTracker.incRemovedInstancesSchemaLinks
            //TODO move this more efficient spot
            //check if old schema element is still needed, delete otherwise from schema graph summary
            if (activeLinks <= 0) {
              graphDatabase.deleteSchemaElement(schemaHash)
              ChangeTracker.getSchemaElementsDeletedThisIteration.add(schemaHash)
            }

            graphDatabase.addNodeFromSchemaElement(vertexID.hashCode, schemaElement.getID)
            ChangeTracker.incAddedInstancesSchemaLinks();
          } else {
            //CASE: instance was known and the schema is the same
            ChangeTracker.incInstancesNotChangedThisIteration()
            graphDatabase.touch(vertexID.hashCode)
          }
        } else {
          //CASE: new instance added
          ChangeTracker.getInstancesNewThisIteration.add(vertexID.hashCode)
          graphDatabase.addNodeFromSchemaElement(vertexID.hashCode, schemaElement.getID)
          ChangeTracker.incAddedInstancesSchemaLinks();
        }


        //always add link to new schema element

        //TODO: maybe move to inner loop above
        val edgeIterator: util.Iterator[(Edge, Edge)] = schemaEdges.iterator
        while (edgeIterator.hasNext) {
          val edgeTuple = edgeIterator.next()
          val optional = graphDatabase.getPreviousLinkID(edgeTuple._1.hashCode)
          if (optional != null) { //instance level edge was known before
            val linkHash: Integer = optional
            if (linkHash != edgeTuple._2.hashCode) { //it was something else before, remove link to old schema edge
              val activeLinks: Integer = graphDatabase.removeEdgeFromSchemaEdge(edgeTuple._1.hashCode, linkHash)
              //TODO move this more efficient spot
              //check if old schema edge is still needed, delete otherwise from schema graph summary
              if (activeLinks <= 0) graphDatabase.deleteSchemaEdge(linkHash)
            }
          }
          //add link to new schema edge
          graphDatabase.addEdgeFromSchemaEdge(edgeTuple._1.hashCode, edgeTuple._2.hashCode)
        }
      }
    }


    graphDatabase.close()
  }

}
