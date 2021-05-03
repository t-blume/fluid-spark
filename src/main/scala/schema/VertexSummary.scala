package schema

import utils.MyHash


class VertexSummary extends Serializable {

  //schema stuff
  var label: Set[String] = Set[String]()
  var neighbors: Map[String, VertexSummary] = Map[String, VertexSummary]()

  //payload stuff
  var payload: java.util.HashSet[String] = new java.util.HashSet[String]()
  //incremental stuff
  var instances: java.util.HashSet[String] = new java.util.HashSet[String]()

  def merge(other: VertexSummary): Unit = {
    label = label ++ other.label
    neighbors = neighbors ++ other.neighbors
    payload.addAll(other.payload)
    instances.addAll(other.instances)
  }

  def getID() : Int = {
    var hashCode: Int = 17
    if (label.size > 0)
      label.foreach(l => hashCode += MyHash.hashString(l))
    hashCode += 31
    if(neighbors.size > 0)
      neighbors.iterator.foreach(elem => hashCode += MyHash.hashString(elem._1) + elem._2.getID())
    hashCode
  }

  override def toString: String = "SE{" + "label=" + label + ", neighbors=" + neighbors + ", payload=" + payload + '}'
}
