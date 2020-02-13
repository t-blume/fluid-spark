package input

import java.io._

import junit.framework.TestCase
import org.apache.spark.graphx.Graph
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

import database.Constants.ALL_LABEL
/**
 * Reads two text files containing the same information, but in different order.
 * Tests if the parsed graph is the same.
 *
 * @author Till Blume, 13.02.2020
 */
class RDFGraphParserTest extends TestCase {
  val testFile = "resources/timbl-500.nq"
  val testFileShuffled = "resources/timbl-500-shuffled.nq"



  def testParseWithType(): Unit = {
    parseBothGraphs("type")
  }

  def testParseWithoutTypes(): Unit = {
    parseBothGraphs(RDFGraphParser.classSignal)
  }

  def testParseWithAllAttributesAsTypes(): Unit = {
    parseBothGraphs(ALL_LABEL)
  }


  def parseBothGraphs(classSignal : String): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("RDFGraphParserTest").
      setMaster("local[*]"))

    var lines: mutable.MutableList[String] = mutable.MutableList()

    val reader: BufferedReader = new BufferedReader(new FileReader(new File(testFile)))
    var line: String = reader.readLine
    while (line != null) {
      lines += line
      line = reader.readLine
    }

    reader.close()

    scala.util.Random.shuffle(lines)
    val writer: BufferedWriter = new BufferedWriter(new FileWriter(new File(testFileShuffled)))
    lines.foreach(l => {
      writer.write(l + "\n")
    })
    writer.flush()
    writer.close()

    val inputEdges1 = sc.textFile(testFile).filter(line => !line.isBlank).map(line => NTripleParser.parse(line))
    val inputEdges2 = sc.textFile(testFileShuffled).filter(line => !line.isBlank).map(line => NTripleParser.parse(line))

    RDFGraphParser.classSignal = classSignal
    val graph: Graph[Set[(String, String)], (String, String, String, String)] = RDFGraphParser.parse(inputEdges1)
    val graph2: Graph[Set[(String, String)], (String, String, String, String)] = RDFGraphParser.parse(inputEdges2)

    graph.cache()
    graph2.cache()


    val vertices1 = graph.vertices.collect()
    val vertices2 = graph2.vertices.collect()

    val edges1 = graph.edges.collect()
    val edges2 = graph2.edges.collect()

    assert(vertices1.length == vertices2.length)
    assert(edges1.length == edges2.length)


    vertices1.foreach(vertex => {
      var foundMatch = false
      if (vertex._2 != null) {
        vertices2.foreach(vertex2 => {
          if (vertex._2 != null && vertex._2.equals(vertex2._2))
            foundMatch = true
        })
        assert(foundMatch)
      }
    })

    edges1.foreach(edge => {
      var foundMatch = false
      if (edge.attr != null) {
        edges2.foreach(edge2 => {
          if (edge.attr != null && edge.attr.equals(edge2.attr))
            foundMatch = true
        })
        assert(foundMatch)
      }
    })
    sc.stop()
  }
}
