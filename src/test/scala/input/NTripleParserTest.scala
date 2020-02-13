package input

import java.util.Random
import java.util.regex.Pattern

import junit.framework.TestCase
import org.apache.spark.graphx.Edge
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Reads a text file containing 500 triples. Tests if the pre-processing properly handles:
 *  - lower-upper case
 *  - trailing '/' and '#'
 *  - alphanumerics
 *
 * @author Till Blume, 13.02.2020
 */
class NTripleParserTest extends TestCase {
  val testFile = "resources/timbl-500.nq"


  def testParse(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("NTripleParserTest").
      setMaster("local[*]"))

    val randomNumber = new Random
    var changedLine: String = null

    sc.textFile(testFile).filter(line => !line.isBlank).collect().
      foreach(line => {
        val edge1: Edge[(String, String, String, String)] = NTripleParser.parse(line)
        val diceRoll = randomNumber.nextInt(6) + 1

        if (diceRoll < 2) { //must still be equal
          changedLine = line.toUpperCase
          val edge2 = NTripleParser.parse(changedLine)
          assert(edge1.attr.equals(edge2.attr))
        }
        else if (diceRoll > 5) {
          val p = Pattern.compile("^(<.*>|_:.*) (<.*>) (<.*>|\".*\"(@.*|\\^\\^<.*>)?|_:.*) (<.*>) \\.$")
          val randomGroup = randomNumber.nextInt(2) + 1

          val m = p.matcher(line)
          if (m.matches) {
            changedLine = line.replaceFirst(m.group(randomGroup), m.group(randomGroup).replace(">", "/>"))
          }
          val edge2 = NTripleParser.parse(changedLine)
          assert(edge1.attr.equals(edge2.attr))
        }
        else if (diceRoll > 4) {
          val p = Pattern.compile("^(<.*>|_:.*) (<.*>) (<.*>|\".*\"(@.*|\\^\\^<.*>)?|_:.*) (<.*>) \\.$")
          val randomGroup = randomNumber.nextInt(2) + 1
          val m = p.matcher(line)
          if (m.matches) {
            changedLine = line.replaceFirst(m.group(randomGroup), m.group(randomGroup).replace(">", "#>"))
          }
          val edge2 = NTripleParser.parse(changedLine)
          assert(edge1.attr.equals(edge2.attr))
        }
        else { //must not be equal
          val p = Pattern.compile("^(<.*>|_:.*) (<.*>) (<.*>|\".*\"(@.*|\\^\\^<.*>)?|_:.*) (<.*>) \\.$")
          val randomGroup = randomNumber.nextInt(2) + 1
          val m = p.matcher(line)

          if (m.matches) {
            changedLine = line.replace(m.group(randomGroup), m.group(randomGroup).replaceFirst("[a-z]", "1"))
          }
          val edge2 = NTripleParser.parse(changedLine)
          if (edge2 != null)
            assert(!edge1.attr.equals(edge2.attr))
        }
      })
      sc.stop()
  }
}
