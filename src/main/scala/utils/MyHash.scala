package utils
import scala.util.hashing.{ MurmurHash3 => MH3 }

object MyHash{
  def hashString(s: String): Int = {
    MH3.stringHash(s, MH3.stringSeed)
  }

  def hashSetOfStrings(s: Set[String]): Int = {
    var hashCode = 0
    s.foreach(elem => hashCode += hashString(elem))
    hashCode
  }
}
