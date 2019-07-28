package database

import java.util

import com.tinkerpop.blueprints.Direction
import schema.SchemaElement
import utils.MyHash

import scala.collection.mutable

class IGSI(database: String, trackChanges: Boolean) extends Serializable {

  val counts = new util.LinkedList[(Int, Long)]()

  def tryAdd(schemaElements: mutable.HashSet[SchemaElement]): Unit = {
    //use one static shared object to access database
    val graphDatabase: OrientDb = OrientDb.getInstance(database, trackChanges)
    val time = System.currentTimeMillis()
    //update instance - schema relations, delete if necessary
    val schemaIterator: Iterator[SchemaElement] = schemaElements.iterator
    var count = 0
    while (schemaIterator.hasNext) {
      val schemaElement = schemaIterator.next()
      //if not already in db, add it (optionally updates payload)
      graphDatabase.writeOrUpdateSchemaElement(schemaElement, true)

      //get vertexID
      val instanceIterator: java.util.Iterator[String] = schemaElement.instances.iterator
      //Note: this is always exactly 1 instance
      while (instanceIterator.hasNext) {
        val vertexID: String = instanceIterator.next
        //check if previously known
        val prevSchemaElement = graphDatabase.getPreviousElement(MyHash.md5HashString(vertexID))
        //        val schemaID = graphDatabase.getPreviousElementID(MyHash.md5HashString(vertexID))
        if (prevSchemaElement != null) { //instance (vertex) was known before
          val schemaHash: Int = prevSchemaElement.getProperty(Constants.PROPERTY_SCHEMA_HASH)
          if (schemaHash != schemaElement.getID()) {
            //CASE: instance was known but with a different schema
            // it was something else before, remove link to old schema element
            if (graphDatabase._changeTracker != null) {
              graphDatabase._changeTracker._instancesWithChangedSchema += 1
              //check if the schema would have been the same if no neighbor information was required
              if (schemaElement.label.hashCode() == prevSchemaElement.getProperty(Constants.PROPERTY_SCHEMA_VALUES).hashCode()){
                  //the label sets are the same
                val iter = prevSchemaElement.getEdges(Direction.OUT, Constants.CLASS_SCHEMA_RELATION).iterator()
                val oldProperties = new java.util.HashSet[String]()
                while (iter.hasNext)
                  oldProperties.add(iter.next().getProperty(Constants.PROPERTY_SCHEMA_VALUES))

                val newProperties: java.util.Set[String] = schemaElement.neighbors.keySet()
                //label are the same and properties are the same, so it must be a neighbor change
                if(oldProperties.hashCode() == newProperties.hashCode())
                  graphDatabase._changeTracker._instancesChangedBecauseOfNeighbors += 1
              }
            }
            //also checks if old schema element is still needed, deleted otherwise
            graphDatabase.removeNodeFromSchemaElement(MyHash.md5HashString(vertexID), schemaHash)
            //create link between instance/payload and schema
            graphDatabase.addNodeToSchemaElement(MyHash.md5HashString(vertexID), schemaElement.getID, schemaElement.payload)
            if (graphDatabase._changeTracker != null)
              graphDatabase._changeTracker._addedInstanceToSchemaLinks += 1
          } else {
            //CASE: instance was known and the schema is the same
            if (graphDatabase._changeTracker != null)
              graphDatabase._changeTracker._instancesNotChanged += 1
            //update timestamp and optionally update payload if it is changed
            graphDatabase.touch(MyHash.md5HashString(vertexID), schemaElement.payload)
          }
        } else {
          //CASE: new instance added
          if (graphDatabase._changeTracker != null)
            graphDatabase._changeTracker._instancesNew += 1
          graphDatabase.addNodeToSchemaElement(MyHash.md5HashString(vertexID), schemaElement.getID, schemaElement.payload)
          if (graphDatabase._changeTracker != null)
            graphDatabase._changeTracker._addedInstanceToSchemaLinks += 1
        }
      }
      count += 1
    }
    val timePassed = System.currentTimeMillis() - time
    counts.add((count, timePassed))
    println((count, timePassed))
  }
}
