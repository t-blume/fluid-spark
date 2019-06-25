package database

import schema.SchemaElement

import scala.collection.mutable

class IGSI(database: String, trackChanges: Boolean) extends Serializable {


  def tryAdd(schemaElements: mutable.HashSet[SchemaElement]): Unit = {
    //use one static shared object to access database
    val graphDatabase: OrientDb = OrientDb.getInstance(database, trackChanges)

    //update instance - schema relations, delete if necessary
    val schemaIterator: Iterator[SchemaElement] = schemaElements.iterator
    while (schemaIterator.hasNext) {
      val schemaElement = schemaIterator.next()
      //if not already in db, add it (optionally updates payload)
      graphDatabase.writeOrUpdateSchemaElement(schemaElement)

      //get vertexID
      val instanceIterator: java.util.Iterator[String] = schemaElement.instances.iterator
      //Note: this is always exactly 1 instance
      while (instanceIterator.hasNext) {
        val vertexID: String = instanceIterator.next
        //check if previously known
        val schemaID = graphDatabase.getPreviousElementID(vertexID.hashCode)
        if (schemaID != null) { //instance (vertex) was known before
          val schemaHash: Integer = schemaID
          if (schemaHash != schemaElement.getID()) {
            //CASE: instance was known but with a different schema
            // it was something else before, remove link to old schema element
            if (graphDatabase._changeTracker != null)
              graphDatabase._changeTracker.getInstancesChangedThisIteration.add(vertexID.hashCode)
            val activeLinks: Integer = graphDatabase.removeNodeFromSchemaElement(vertexID.hashCode, schemaHash)
            if (graphDatabase._changeTracker != null)
              graphDatabase._changeTracker.incRemovedInstancesSchemaLinks
            //TODO move this more efficient spot
            //check if old schema element is still needed, delete otherwise from schema _graph summary
            if (activeLinks <= 0) {
              graphDatabase.deleteSchemaElement(schemaHash)
              if (graphDatabase._changeTracker != null)
                graphDatabase._changeTracker.getSchemaElementsDeletedThisIteration.add(schemaHash)
            }
            //create link between instance/payload and schema
            graphDatabase.addNodeToSchemaElement(vertexID.hashCode, schemaElement.getID, schemaElement.payload)
            if (graphDatabase._changeTracker != null)
              graphDatabase._changeTracker.incAddedInstancesSchemaLinks()
          } else {
            //CASE: instance was known and the schema is the same
            if (graphDatabase._changeTracker != null)
              graphDatabase._changeTracker.incInstancesNotChangedThisIteration()
            //update timestamp and optionally update payload if it is changed
            graphDatabase.touch(vertexID.hashCode, schemaElement.payload)
          }
        } else {
          //CASE: new instance added
          if (graphDatabase._changeTracker != null)
            graphDatabase._changeTracker.getInstancesNewThisIteration.add(vertexID.hashCode)
          graphDatabase.addNodeToSchemaElement(vertexID.hashCode, schemaElement.getID, schemaElement.payload)
          if (graphDatabase._changeTracker != null)
            graphDatabase._changeTracker.incAddedInstancesSchemaLinks()
        }
      }
    }
  }
}
