import java.util

import com.orientechnologies.orient.core.exception.OSchemaException
import com.orientechnologies.orient.core.metadata.schema.OType
import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import graph.Edge
import schema.ISchemaElement


class OrientDBScala(url: String, username: String, password: String) {

  val SE_ID = "hash"
  val SL_ID = "hash"

  val CLASS_SCHEMA_ELEMENT = "class:SE"
  val CLASS_SCHEMA_LINK= "class:SL"

  val PROPERTY_VALUES = "values"
  val PROPERTY_SCHEMA_RELATION = "schema-relation"

  val VERTEX_LABEL = "label"

  val EMPTY_SCHEMA_ELEMENT_HASH: Int = "EMPTY_SCHEMA_ELEMENT_ID".hashCode


  object Factory {
    def getFoo(url: String, username: String, password: String): Unit = {
      new OrientDBScala(url, username, password)
    }
  }

  val graph: OrientGraph = new OrientGraph(url)

  def initSchema(): Unit = {
    try {
      val schemaElement = graph.createVertexType("SE")
      schemaElement.createProperty("hash", OType.INTEGER)
      graph.commit()
      val schemaRelation = graph.createEdgeType("schema-relation")
      graph.commit()
    }
  }

  def deleteSchemaElement(schemaHash: Integer): Unit = {
    graph.getVertices(SE_ID, schemaHash).forEach(_ => graph.removeVertex(_))
    graph.commit()
  }

  def deleteSchemaEdge(linkHash: Integer): Unit = {
    graph.getEdges(SL_ID, linkHash).forEach(_ => graph.removeEdge(_))
    graph.commit()
  }

  def exists(schemaHash: Integer): Boolean = {
    val iterator: util.Iterator[Vertex] = graph.getVertices(SE_ID, schemaHash).iterator
    if (iterator.hasNext) true
    else false
  }

  def writeSchemaElementWithEdges(schemaElement: ISchemaElement): Unit = {
    val vertex: Vertex = graph.addVertex(CLASS_SCHEMA_ELEMENT, Nil: _*)
    vertex.setProperty(SE_ID, schemaElement.getID)
    //TODO: check if this is a good idea
    vertex.setProperty(PROPERTY_VALUES, schemaElement.getLabel)

    val iterator: util.Iterator[(Edge, Edge)] = schemaElement.getSchemaEdges.iterator

    while (iterator.hasNext){
      val schemaEdge = iterator.next()._2
      val endID: Integer = if (schemaEdge.end == null) EMPTY_SCHEMA_ELEMENT_HASH
      else Integer.valueOf(schemaEdge.end)
      var targetV: Vertex = getVertexByHashID(endID)
      if (targetV == null) { //This node does not yet exist, so create one (Hint: if it is a complex schema, you will need to add information about this one later)
        targetV = graph.addVertex(CLASS_SCHEMA_ELEMENT, Nil: _*)
        targetV.setProperty(SE_ID, endID)
      }
      vertex.addEdge(PROPERTY_SCHEMA_RELATION, targetV)
      graph.commit() //TODO: check if this is a good place

    }
  }

  def getVertexByHashID(schemaHash: Integer): Vertex = {
    val iterator: util.Iterator[Vertex] = graph.getVertices(SE_ID, schemaHash).iterator
    if (iterator.hasNext) iterator.next
    else null
  }

  def deletePayloadElement(payloadHash: Integer): Unit = {
  }

  def deletePayloadEdge(linkHash: Integer): Unit = {
  }

  def close(): Unit = {
    graph.shutdown()
  }

}
