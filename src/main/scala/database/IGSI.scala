package database

import java.util

import com.tinkerpop.blueprints.Direction
import schema.SchemaElement
import utils.MyHash

import scala.collection.mutable

class IGSI(database: String, trackChanges: Boolean) extends Serializable {

  val counts = new util.LinkedList[(Int, Long)]()

  def tryAddOptimized(schemaElements: mutable.HashSet[SchemaElement]): Unit = {
    //use one static shared object to access database (multithreading inside object)
    val graphDatabase: OrientDbOptwithMem = OrientDbOptwithMem.getInstance(database, trackChanges)
    //get all summarized instances
    val instances = new util.HashMap[String, util.HashSet[String]]()
    for (se: SchemaElement <- schemaElements) {
      //get vertexID
      val instanceIterator: java.util.Iterator[String] = se.instances.iterator
      //Note: this is always exactly 1 instance
      while (instanceIterator.hasNext)
        instances.put(instanceIterator.next(), se.payload)
    }
    //when there is more than one schema element, then it is the same schema element with different summarized instances
    val schemaElement = schemaElements.iterator.next()

    //if not already in db, add it (optionally updates payload)
    val instanceIds = new util.HashSet[Integer]()
    instances.keySet().forEach(K => instanceIds.add(MyHash.md5HashString(K)))
    graphDatabase.writeOrUpdateSchemaElement(schemaElement, instanceIds, true)

    //get vertexID
    val instanceIterator = instances.entrySet().iterator()

    /*
      collect all Updates and perform them in a micro batch
     */
    val nodesTobeAdded: util.HashMap[Integer, util.Set[String]] = new util.HashMap[Integer, util.Set[String]]()
    val nodesTobeTouched: util.HashMap[Integer, util.Set[String]] = new util.HashMap[Integer, util.Set[String]]()
    val nodesTobeRemoved: util.HashMap[Integer, Integer] = new util.HashMap[Integer, Integer]()
    while (instanceIterator.hasNext) {
      val next = instanceIterator.next
      val vertexID: String = next.getKey
      //check if previously known
      val prevSchemaHash = graphDatabase.getPreviousElementID(MyHash.md5HashString(vertexID))
      if (prevSchemaHash != null) {
        //instance (vertex) was known before
        if (prevSchemaHash != schemaElement.getID()) {
          //CASE: instance was known but with a different schema
          // it was something else before, remove link to old schema element
          if (trackChanges) {
            val prevSchemaElement = graphDatabase.getVertexByHashID(Constants.PROPERTY_SCHEMA_HASH, prevSchemaHash);
            ChangeTracker.getInstance().incInstancesWithChangedSchema()
            //check if the schema would have been the same if no neighbor information was required
            try {
              if (prevSchemaElement != null &&
                (schemaElement.label == null &&
                  prevSchemaElement.getProperty(Constants.PROPERTY_SCHEMA_VALUES) == null) || (
                schemaElement.label != null &&
                  prevSchemaElement.getProperty(Constants.PROPERTY_SCHEMA_VALUES) != null &&
                  schemaElement.label.hashCode() == prevSchemaElement.getProperty(Constants.PROPERTY_SCHEMA_VALUES).hashCode())) {
                //the label sets are the same
                val iter = prevSchemaElement.getEdges(Direction.OUT, Constants.CLASS_SCHEMA_RELATION).iterator()
                val oldProperties = new java.util.HashSet[String]()
                while (iter.hasNext)
                  oldProperties.add(iter.next().getProperty(Constants.PROPERTY_SCHEMA_VALUES))

                val newProperties: java.util.Set[String] = schemaElement.neighbors.keySet()
                //label are the same and properties are the same, so it must be a neighbor change
                if (oldProperties.hashCode() == newProperties.hashCode())
                  ChangeTracker.getInstance().incInstancesChangedBecauseOfNeighbors()
              }
            } catch {
              case ex: NullPointerException => {
                println("WHAT THE FUCK?")
              }
            }
          }
          //also checks if old schema element is still needed, deleted otherwise
          nodesTobeRemoved.put(MyHash.md5HashString(vertexID), prevSchemaHash)
          //create link between instance/payload and schema
          nodesTobeAdded.put(MyHash.md5HashString(vertexID), next.getValue)
        } else {
          //CASE: instance was known and the schema is the same
          //update timestamp and optionally update payload if it is changed
          nodesTobeTouched.put(MyHash.md5HashString(vertexID), next.getValue)
          println(MyHash.md5HashString(vertexID))
        }
      } else {
        //CASE: new instance added
        nodesTobeAdded.put(MyHash.md5HashString(vertexID), next.getValue)
      }
    }
    graphDatabase.addNodesToSchemaElement(nodesTobeAdded, schemaElement.getID())
    graphDatabase.touchMultiple(nodesTobeTouched)
    graphDatabase.removeNodesFromSchemaElement(nodesTobeRemoved)
  }
}
