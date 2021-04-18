package schema

import utils.MyHash
class VertexSummary extends Serializable {

  //schema stuff
  var label: java.util.HashSet[String] = new java.util.HashSet[String]()
  var neighbors: java.util.HashMap[String, VertexSummary] = new java.util.HashMap[String, VertexSummary]()
  //payload stuff
  var payload: java.util.HashSet[String] = new java.util.HashSet[String]()
  //incremental stuff
  var instances: java.util.HashSet[String] = new java.util.HashSet[String]()

  def merge(other: VertexSummary) : Unit = {
      label.addAll(other.label)
      neighbors.putAll(other.neighbors)
      payload.addAll(other.payload)
      instances.addAll(other.instances)
  }

  def getID() : Int = {
    var hashCode: Int = 17
    if (label.size() > 0)
      label.forEach(l => hashCode += MyHash.hashString(l))
    hashCode += 31
    if(neighbors.size() > 0)
      neighbors.forEach((K,V) => hashCode += MyHash.hashString(K) + V.getID())
    hashCode
  }

  override def toString: String = "SE{" + "label=" + label + ", neighbors=" + neighbors + ", payload=" + payload + '}'
}
