//package input
//
//import java.io._
//
//import junit.framework.TestCase
//import org.apache.spark.graphx.Graph
//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.collection.mutable
//
//class RDFGraphParserTest extends TestCase {
//  val testFile = "resources/timbl-500.nq"
//  val testFileShuffled = "resources/timbl-500-shuffled.nq"
//  val sc = new SparkContext(new SparkConf().setAppName("RDFGraphParserTest").
//    setMaster("local[4]"))
//
////  val parser = new NTripleParser()
//
//  def testParse(): Unit = {
//    var lines: mutable.MutableList[String] = mutable.MutableList()
//
//
//    val reader: BufferedReader = new BufferedReader(new FileReader(new File(testFile)))
//    var line: String = reader.readLine
//    while (line != null) {
//      lines += line
//      line = reader.readLine
//    }
//
//    reader.close()
//
//    scala.util.Random.shuffle(lines)
//    val writer: BufferedWriter = new BufferedWriter(new FileWriter(new File(testFileShuffled)))
//    lines.foreach(l => {
//      writer.write(l + "\n")
//    })
//    writer.flush()
//    writer.close()
//
//    val edges = sc.textFile(testFile).filter(line => !line.isBlank).map(line => NTripleParser.parse(line))
//    val edges2 = sc.textFile(testFileShuffled).filter(line => !line.isBlank).map(line => NTripleParser.parse(line))
//
//    val graph: Graph[Set[(String, String)], (String, String, String, String)] = RDFGraphParser.parse(edges)
//    val graph2: Graph[Set[(String, String)], (String, String, String, String)] = RDFGraphParser.parse(edges2)
//
//    graph.cache()
//    graph2.cache()
//    assert(graph.vertices.count == graph2.vertices.count)
//    assert(graph.edges.count == graph2.edges.count)
//
//    graph.vertices.foreach(vertex => {
//      var foundMatch = false
//      if (vertex._2 != null) {
//        graph2.vertices.foreach(vertex2 => {
//          if (vertex._2 != null && vertex._2.equals(vertex2._2))
//            foundMatch = true
//        })
//        assert(foundMatch)
//      }
//
//    })
//
//    graph.edges.collect().foreach(edge => {
//      var foundMatch = false
//      assert(graph2.edges.collect() != null)
//
//      if (edge.attr != null) {
//        graph2.edges.collect().foreach(edge2 => {
//          if (edge.attr != null && edge.attr.equals(edge2.attr))
//            foundMatch = true
//        })
//        assert(foundMatch)
//      }
//    })
//  }
//}
