package schema

import utils.MyHash
class VertexSummary extends Serializable {

  //schema stuff
  var label: java.util.HashSet[String] = new java.util.HashSet[String]()
  var neighbors: java.util.HashMap[String, SchemaElement] = new java.util.HashMap[String, SchemaElement]()
  //payload stuff
  var payload: java.util.HashSet[String] = new java.util.HashSet[String]()
  //incremental stuff
  var instances: java.util.HashSet[String] = new java.util.HashSet[String]()

  def static_merge(a: SchemaElement, b: SchemaElement): SchemaElement = {
    a.merge(b)
    a
  }

  def neighbor_update(a: SchemaElement, b: SchemaElement): SchemaElement = {
    b.neighbors.forEach((p, e) => {
      a.neighbors.put(p, e)
    })
    a
  }
  def merge(other: SchemaElement): Unit = {
    label.addAll(other.label)

    other.neighbors.forEach((p, e) => {
      neighbors.merge(p, e, static_merge)
    })

    payload.addAll(other.payload)
    instances.addAll(other.instances)
  }

  def getID() : Int = {
    var hashCode: Int = 17
    if (label.size() > 0)
      label.forEach(l => hashCode += MyHash.md5HashString(l))
    hashCode += 31
    if(neighbors.size() > 0)
      neighbors.forEach((K,V) => hashCode += MyHash.md5HashString(K) + V.getID())
    hashCode
  }

  override def toString: String = "SE{" + "label=" + label + ", neighbors=" + neighbors + ", payload=" + payload + '}'
}
