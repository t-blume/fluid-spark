package classes

import database.OrientDb
import schema.ChangeTracker

import scala.collection.mutable

class IGSI() extends Serializable {


  def tryAdd(schemaElements: mutable.HashSet[SchemaElement]): Unit = {
    //use one static shared object to access database
    val graphDatabase: OrientDb = OrientDb.getInstance()

    //update instance - schema relations, delete if necessary
    val schemaIterator: Iterator[SchemaElement] = schemaElements.iterator
    while (schemaIterator.hasNext) {
      val schemaElement = schemaIterator.next()
      //if not already in db, add it (optionally updates payload)
      graphDatabase.writeSchemaElementWithEdges(schemaElement)

      //get vertexID
      val instanceIterator: java.util.Iterator[String] = schemaElement.instances.iterator
      while (instanceIterator.hasNext) {
        val vertexID: String = instanceIterator.next
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
            ChangeTracker.incAddedInstancesSchemaLinks()
          } else {
            //CASE: instance was known and the schema is the same
            ChangeTracker.incInstancesNotChangedThisIteration()
            graphDatabase.touch(vertexID.hashCode)
          }
        } else {
          //CASE: new instance added
          ChangeTracker.getInstancesNewThisIteration.add(vertexID.hashCode)
          graphDatabase.addNodeFromSchemaElement(vertexID.hashCode, schemaElement.getID)
          ChangeTracker.incAddedInstancesSchemaLinks()
        }
      }
    }
  }

}
