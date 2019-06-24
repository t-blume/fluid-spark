package input

import java.util.regex.Pattern

import org.apache.spark.graphx.Edge

object NTripleParser {
  val baseURI = "http://informatik.uni-kiel.de/fluid#"

  def parse(line: String): Edge[(String, String, String, String)] = {
    val p = Pattern.compile("^(<.*>|_:.*) (<.*>) (<.*>|\".*\"(@.*|\\^\\^<.*>)?|_:.*) (<.*>) \\.$")
    val m = p.matcher(line)

    if (m.matches) {
      val start = parseNode(m.group(1).replaceAll("<|>", ""))
      val label = parseNode(m.group(2).replaceAll("<|>", ""))
      val end = parseNode(m.group(3).replaceAll("<|>", ""))
      val source = parseNode(m.group(5).replaceAll("<|>", ""))
      val edge = new Edge[(String, String, String, String)](start.hashCode, end.hashCode, (start, label, end, source))
      return edge
    }
    else return null
  }

  def parseNode(node: String): String = {
    var parsedNode = node
    if (parsedNode.startsWith("_:"))
      parsedNode = parsedNode.replaceFirst("_:", baseURI)
    while (parsedNode.matches((".*(\\/+|#+)$"))) {
      while (parsedNode.endsWith("/"))
        parsedNode = parsedNode.substring(0, parsedNode.length - 1)

      while (parsedNode.endsWith("#"))
        parsedNode = parsedNode.substring(0, parsedNode.length - 1)
    }
    return parsedNode.toLowerCase
  }
}
