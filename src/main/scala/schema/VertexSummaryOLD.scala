package schema

import schema.SE_ComplexAttributeClassCollectionBisim.static_merge
import utils.MyHash


class VertexSummaryOLD extends Serializable {

  //schema stuff
  var label: java.util.HashSet[String] = new java.util.HashSet[String]()
  var neighbors: java.util.HashMap[String, VertexSummaryOLD] = new java.util.HashMap[String, VertexSummaryOLD]()
  //payload stuff
  var payload: java.util.HashSet[String] = new java.util.HashSet[String]()
  //incremental stuff
  var instances: java.util.HashSet[String] = new java.util.HashSet[String]()

  var inProp: String = null


  def merge(other: VertexSummaryOLD): Unit = {
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
      label.forEach(l => hashCode += MyHash.hashString(l))
    hashCode += 31
    if(neighbors.size() > 0)
      neighbors.forEach((K,V) => hashCode += MyHash.hashString(K) + V.getID())
    hashCode
  }

  override def toString: String = "SE{" + "label=" + label + ", neighbors=" + neighbors + ", payload=" + payload + '}'
}
