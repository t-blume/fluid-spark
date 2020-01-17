package schema


import utils.MyHash

import scala.beans.BeanProperty
import scala.collection.mutable

class SchemaElement extends Serializable {

  object VALS {
    val EMPTY_SCHEMA_ELEMENT = new SchemaElement
  }

  //schema stuff
  @BeanProperty
  var label: mutable.HashSet[String] = new mutable.HashSet[String]()
  var neighbors: mutable.HashMap[String, SchemaElement] = new mutable.HashMap[String, SchemaElement]()
  //payload stuff
  @BeanProperty
  var payload: mutable.HashSet[String] = new mutable.HashSet[String]()
  //incremental stuff
  var instances: mutable.HashSet[String] = new mutable.HashSet[String]()

  def merge(other: SchemaElement) : Unit = {
      other.label.foreach(l => label.add(l))
      other.neighbors.foreach(e => neighbors.put(e._1, e._2))
      other.payload.foreach(p => payload.add(p))
      other.instances.foreach(i => instances.add(i))
  }

  def getID() : Int = {
    var hashCode = 17
    label.foreach(l => hashCode += MyHash.md5HashString(l))
    hashCode += 31
    neighbors.foreach(e => hashCode += MyHash.md5HashString(e._1) + e._2.getID())
    hashCode
  }

  override def toString: String = "SE{" + "label=" + label + ", neighbors=" + neighbors + ", payload=" + payload + '}'

}
