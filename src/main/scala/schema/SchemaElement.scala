package schema

import utils.MyHash
class SchemaElement extends Serializable {

  object VALS {
    val EMPTY_SCHEMA_ELEMENT = new SchemaElement
  }

  //schema stuff
  var label: java.util.HashSet[String] = new java.util.HashSet[String]()
  var neighbors: java.util.HashMap[String, SchemaElement] = new java.util.HashMap[String, SchemaElement]()
  //payload stuff
  var payload: java.util.HashSet[String] = new java.util.HashSet[String]()
  //incremental stuff
  var instances: java.util.HashSet[String] = new java.util.HashSet[String]()

  def merge(other: SchemaElement) : Unit = {
      label.addAll(other.label)
      neighbors.putAll(other.neighbors)
      payload.addAll(other.payload)
      instances.addAll(other.instances)
  }

  def getID() : Int = {
    var hashCode = 17
    label.forEach(l => hashCode += MyHash.md5HashString(l))
    hashCode += 31
    neighbors.forEach((K,V) => hashCode += MyHash.md5HashString(K) + V.getID())
    hashCode
  }

  override def toString: String = "SE{" + "label=" + label + ", neighbors=" + neighbors + ", payload=" + payload + '}'
}
