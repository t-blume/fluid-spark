package input

import java.util.regex.Pattern

import org.apache.spark.graphx.Edge
import utils.MyHash

//class NTripleParser(baseURI: String = "http://informatik.uni-kiel.de/fluid#", defaultSource: String = "http://informatik.uni-kiel.de") extends Serializable {
object NTripleParser {
  var baseURI: String = "http://informatik.uni-kiel.de/fluid#"
  var defaultSource: String = "http://informatik.uni-kiel.de"

  def parse(line: String): Edge[(String, String, String, String)] = {
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
      parsedNode.toLowerCase
    }

    val p = Pattern.compile("^(<.*>|_:.*) (<.*>) (<.*>|\".*\"(@.*|\\^\\^<.*>)?|_:.*) (<.*>) \\.$")
    val m = p.matcher(line)

    if (m.matches) {
      //n-quad
      val start = parseNode(m.group(1).replaceAll("<|>", ""))
      val label = parseNode(m.group(2).replaceAll("<|>", ""))
      val end = parseNode(m.group(3).replaceAll("<|>", ""))
      val source = parseNode(m.group(5).replaceAll("<|>", ""))
      val edge = new Edge[(String, String, String, String)](MyHash.md5HashString(start), MyHash.md5HashString(end), (start, label, end, source))
      return edge
    }
    val p2 = Pattern.compile("^(<.*>|_:.*) (<.*>) (<.*>|\".*\"(@.*|\\^\\^<.*>)?|_:.*) \\.$")
    val m2 = p2.matcher(line)

    if (m2.matches) {
      //n-triple
      val start = parseNode(m2.group(1).replaceAll("<|>", ""))
      val label = parseNode(m2.group(2).replaceAll("<|>", ""))
      val end = parseNode(m2.group(3).replaceAll("<|>", ""))

      val edge = new Edge[(String, String, String, String)](MyHash.md5HashString(start), MyHash.md5HashString(end), (start, label, end, defaultSource))
      return edge
    }

    new Edge[(String, String, String, String)]()
  }


}
