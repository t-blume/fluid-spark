package schema

import input.{NTripleParser, RDFGraphParser}
import junit.framework.TestCase
import org.apache.spark.graphx.Graph
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * Tests if all schema extraction methods return the expected schema elements (summary graphs).
 *
 * @author Till Blume, 13.02.2020
 */
class SchemaExtractionTest extends TestCase {
  val testFileCorrectness = "resources/manual-test-0.nq"

  def testSE_AttributeCollectionUndirected(): Unit = {
    //create gold standard
    /*
    <tbl> <type> <Person> <http://zbw.eu> .
    <tbl> <name> "Till Blume" <http://zbw.eu> .
    <tbl> <worksFor> <ZBW> <http://zbw.eu> .
    <tbl> <livesIn> "Kiel" <http://zbw.eu> .
    <ZBW> <type> <Organisation> <http://zbw.eu> .
    <ZBW> <name> "Leibniz Information Centre for Economics" <http://zbw.eu> .
     */

    val schemaElement1 = new VertexSummary
    schemaElement1.payload.add("http://zbw.eu")
    schemaElement1.instances.add("tbl")
    schemaElement1.neighbors + ("worksfor" -> new VertexSummary)
    schemaElement1.neighbors + ("name" -> new VertexSummary)
    schemaElement1.neighbors + ("livesin" -> new VertexSummary)

    val schemaElement2 = new VertexSummary
    schemaElement2.instances.add("zbw")
    schemaElement2.payload.add("http://zbw.eu")
    schemaElement2.neighbors + ("name" -> new VertexSummary)
    schemaElement2.neighbors + ("worksfor" -> new VertexSummary)

    val goldElements = List(schemaElement1, schemaElement2)


    val sc = new SparkContext(new SparkConf().setAppName("SchemaExtractionTest").
      setMaster("local[*]"))

    val graph = parseGraph(sc, true)

    val schemaExtraction: SchemaExtraction = SE_AttributeCollection

    val checkElements = runSchemaExtraction(graph, schemaExtraction)

    validate(goldElements, checkElements)
    sc.stop()
  }


  def testSE_AttributeCollection(): Unit = {
    //create gold standard
    /*
    <tbl> <type> <Person> <http://zbw.eu> .
    <tbl> <name> "Till Blume" <http://zbw.eu> .
    <tbl> <worksFor> <ZBW> <http://zbw.eu> .
    <tbl> <livesIn> "Kiel" <http://zbw.eu> .
    <ZBW> <type> <Organisation> <http://zbw.eu> .
    <ZBW> <name> "Leibniz Information Centre for Economics" <http://zbw.eu> .
     */

    val schemaElement1 = new VertexSummary
    schemaElement1.payload.add("http://zbw.eu")
    schemaElement1.instances.add("tbl")
    schemaElement1.neighbors + ("worksfor" -> new VertexSummary)
    schemaElement1.neighbors + ("name" -> new VertexSummary)
    schemaElement1.neighbors + ("livesin" -> new VertexSummary)

    val schemaElement2 = new VertexSummary
    schemaElement2.instances.add("zbw")
    schemaElement2.payload.add("http://zbw.eu")
    schemaElement2.neighbors + ("name" -> new VertexSummary)

    val goldElements = List(schemaElement1, schemaElement2)


    val sc = new SparkContext(new SparkConf().setAppName("SchemaExtractionTest").
      setMaster("local[*]"))

    val graph = parseGraph(sc, false)

    val schemaExtraction: SchemaExtraction = SE_AttributeCollection

    val checkElements = runSchemaExtraction(graph, schemaExtraction)

    validate(goldElements, checkElements)
    sc.stop()
  }



  def testSE_ClassCollection(): Unit = {
    //create gold standard
    /*
    <tbl> <type> <Person> <http://zbw.eu> .
    <tbl> <name> "Till Blume" <http://zbw.eu> .
    <tbl> <worksFor> <ZBW> <http://zbw.eu> .
    <tbl> <livesIn> "Kiel" <http://zbw.eu> .
    <ZBW> <type> <Organisation> <http://zbw.eu> .
    <ZBW> <name> "Leibniz Information Centre for Economics" <http://zbw.eu> .
     */

    val schemaElement1 = new VertexSummary
    schemaElement1.label += "person"
    schemaElement1.payload.add("http://zbw.eu")
    schemaElement1.instances.add("tbl")

    val schemaElement2 = new VertexSummary
    schemaElement2.label += "organisation"
    schemaElement2.instances.add("zbw")
    schemaElement2.payload.add("http://zbw.eu")

    val goldElements = List(schemaElement1, schemaElement2)


    val sc = new SparkContext(new SparkConf().setAppName("SchemaExtractionTest").
      setMaster("local[*]"))

    val graph = parseGraph(sc, false)

    val schemaExtraction: SchemaExtraction = SE_ClassCollection

    val checkElements = runSchemaExtraction(graph, schemaExtraction)

    validate(goldElements, checkElements)
    sc.stop()
  }


  def testSE_ComplexAttributeClassCollection(): Unit = {
    //create gold standard
    /*
    <tbl> <type> <Person> <http://zbw.eu> .
    <tbl> <name> "Till Blume" <http://zbw.eu> .
    <tbl> <worksFor> <ZBW> <http://zbw.eu> .
    <tbl> <livesIn> "Kiel" <http://zbw.eu> .
    <ZBW> <type> <Organisation> <http://zbw.eu> .
    <ZBW> <name> "Leibniz Information Centre for Economics" <http://zbw.eu> .
     */

    val schemaElement1 = new VertexSummary
    schemaElement1.label += "person"
    schemaElement1.payload.add("http://zbw.eu")
    schemaElement1.instances.add("tbl")
    val objectElement1 = new VertexSummary
    objectElement1.label += "organisation"
    schemaElement1.neighbors + ("worksfor" -> objectElement1)
    schemaElement1.neighbors + ("name" -> new VertexSummary)
    schemaElement1.neighbors + ("livesin" -> new VertexSummary)

    val schemaElement2 = new VertexSummary
    schemaElement2.label += "organisation"
    schemaElement2.instances.add("zbw")
    schemaElement2.payload.add("http://zbw.eu")
    schemaElement2.neighbors + ("name" -> new VertexSummary)

    val goldElements = List(schemaElement1, schemaElement2)


    val sc = new SparkContext(new SparkConf().setAppName("SchemaExtractionTest").
      setMaster("local[*]"))

    val graph = parseGraph(sc, false)

    val schemaExtraction: SchemaExtraction = SE_ComplexAttributeClassCollection

    val checkElements = runSchemaExtraction(graph, schemaExtraction)

    validate(goldElements, checkElements)
    sc.stop()
  }


  def parseGraph(sc: SparkContext, undirected: Boolean): Graph[Set[(String, String)], (String, String, String, String)] = {
    RDFGraphParser.classSignal = "type"
    RDFGraphParser.useIncoming = undirected
    //parse n-triple file to RDD of GraphX Edges
    val edges = sc.textFile(testFileCorrectness).filter(line => !line.isBlank).map(line => NTripleParser.parse(line))
    //build _graph from vertices and edges from edges
    RDFGraphParser.parse(edges)
  }
  /**
   * Schema Summarization:
   */
  def runSchemaExtraction(graph: Graph[Set[(String, String)], (String, String, String, String)],
                          schemaExtraction: SchemaExtraction): Array[mutable.HashSet[VertexSummary]] = {

    val schemaElements = graph.aggregateMessages[(Int, mutable.HashSet[VertexSummary])](
      triplet => schemaExtraction.sendMessage(triplet),
      (a, b) => schemaExtraction.mergeMessage(a, b))

    schemaElements.values.reduceByKey(_ ++ _).values.collect()
  }



  def validate(goldElements: List[VertexSummary], checkElements: Array[mutable.HashSet[VertexSummary]]): Unit = {
    assert(goldElements.size == checkElements.length)

    val goldIterator: Iterator[VertexSummary] = goldElements.iterator
    while (goldIterator.hasNext){
      val seGold = goldIterator.next()
      var foundMatch = false
      val checkIterator: Iterator[mutable.HashSet[VertexSummary]] = checkElements.iterator
      while (checkIterator.hasNext){
        //retrieve first form stack of equivalent schema elements
        val seCheck = checkIterator.next().iterator.next()
        if(seGold.getID().equals(seCheck.getID()))
          foundMatch = true
      }
      if(!foundMatch) {
        println("SE_GOLD: " + seGold)
        println("SE_GOLD: " + seGold.getID())
      }
      assert(foundMatch)
    }
  }
}
