package classes
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
      neighbors.putAll(other.neighbors) //TODO: verify that this does not delete stuff
      payload.addAll(other.payload)
      instances.addAll(other.instances)
  }

  def getID() : Int = {
    17 + label.hashCode() + 31 + neighbors.hashCode()
  }

  override def toString: String = "SE{" + "label=" + label + ", neighbors=" + neighbors + ", payload=" + payload + '}'
}
