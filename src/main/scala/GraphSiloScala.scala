import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

class GraphSiloScala extends Serializable {

  private val nodeIDtoSchemaHash = new HashMap[Integer, Integer]
  private val schemaHashtoNodeIDs = new HashMap[Integer, HashSet[Integer]]


  private val edgeIDtoLinkHash = new HashMap[Integer, Integer]
  private val linkHashtoEdgeIDs = new HashMap[Integer, HashSet[Integer]]

  /**
    * return id or null if not present
    *
    * @param nodeID
    * @return
    */
  def getPreviousElementID(nodeID: Integer): Option[Integer] = nodeIDtoSchemaHash.get(nodeID)

  def removeNodeFromSchemaElement(nodeID: Integer, schemaHash: Integer): Integer = {
    val optional:Option[HashSet[Integer]] = schemaHashtoNodeIDs.get(schemaHash)
    if (optional.isDefined) {
      var nodeIDs: HashSet[Integer] = optional.get
      nodeIDs.remove(nodeID)
      if (nodeIDs.size <= 0) schemaHashtoNodeIDs.remove(schemaHash)
      else schemaHashtoNodeIDs.put(schemaHash, nodeIDs) //TODO needed?
      nodeIDs.size
    }
    else 0
  }

  def addNodeFromSchemaElement(nodeID: Integer, schemaHash: Integer): Integer = {
    var optional = schemaHashtoNodeIDs.get(schemaHash)
    var nodeIDs = new HashSet[Integer]
    if (optional.isDefined) nodeIDs = optional.get

    nodeIDs.add(nodeID)
    schemaHashtoNodeIDs.put(schemaHash, nodeIDs)
    nodeIDs.size
  }

  def getPreviousLinkID(edgeID: Integer): Option[Integer] = edgeIDtoLinkHash.get(edgeID)

  def removeEdgeFromSchemaEdge(edgeID: Integer, linkHash: Integer): Integer = {
    val optional = linkHashtoEdgeIDs.get(linkHash)
    if (optional.isDefined) {
      val edgeIDs = optional.get
      edgeIDs.remove(edgeID)
      if (edgeIDs.size <= 0) linkHashtoEdgeIDs.remove(linkHash)
      else linkHashtoEdgeIDs.put(linkHash, edgeIDs)
      edgeIDs.size
    }
    else 0
  }

  def addEdgeFromSchemaEdge(edgeID: Integer, linkHash: Integer): Integer = {
    val optional = linkHashtoEdgeIDs.get(linkHash)
    var edgeIDs = new HashSet[Integer]
    if (optional.isDefined) edgeIDs = optional.get
    edgeIDs.add(edgeID)
    linkHashtoEdgeIDs.put(linkHash, edgeIDs)
    edgeIDs.size
  }
}
