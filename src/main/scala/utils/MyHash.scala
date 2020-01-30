package utils

object MyHash {
  def md5HashString(s: String): Int = {
    import scala.util.hashing.{ MurmurHash3 => MH3 }
    MH3.stringHash(s, MH3.stringSeed)
  }

  def md5HashImprintRelation(imprintID: Int, schemaID: Int): Int = {
    md5HashString(imprintID.toString.concat(schemaID.toString))
  }
}
