import java.util.regex.Pattern

import org.apache.spark.graphx.Edge

object NTripleParser {
  val baseURI = "http://informatik.uni-kiel.de/fluid#"

  def parse(line: String): Edge[(String, String, String, String)] = {
    val p = Pattern.compile("^(<.*>|_:.*) (<.*>) (<.*>|\".*\"(@.*|\\^\\^<.*>)?|_:.*) (<.*>) \\.$")
    val m = p.matcher(line)

    if (m.matches) {
      var start = m.group(1).replaceAll("<|>", "").toLowerCase
      if(start.startsWith("_:"))
        start = start.replaceFirst("_:", baseURI)

      val label = m.group(2).replaceAll("<|>", "").toLowerCase
      val end = m.group(3).replaceAll("<|>", "").toLowerCase
      val source = m.group(5).replaceAll("<|>", "").toLowerCase

      val edge = new Edge[(String, String, String, String)](start.hashCode, end.hashCode, (start, label, end, source))
      return edge
    }
    else return null
  }
}
