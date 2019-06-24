package schema

import classes.SchemaElement
import input.{NTripleParser, RDFGraphParser}
import junit.framework.TestCase
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext}

class SchemaExtractionTest extends TestCase {
  val testFile = "resources/manual-test-1.nq"
  val sc = new SparkContext(new SparkConf().setAppName("SchemaExtractionTest").
    setMaster("local[4]"))


  def test(): Unit = {
    //create gold standard
    /*
    <tbl> <type> <Person> <http://zbw.eu> .
    <tbl> <name> "Till Blume" <http://zbw.eu> .
    <tbl> <worksFor> <ZBW> <http://zbw.eu> .
    <tbl> <livesIn> "Kiel" <http://zbw.eu> .
    <ZBW> <type> <Organisation> <http://zbw.eu> .
    <ZBW> <name> "Leibniz Information Centre for Economics" <http://zbw.eu> .
     */

    val schemaElement1 = new SchemaElement
    schemaElement1.label.add("person")
    schemaElement1.payload.add("http://zbw.eu")
    schemaElement1.instances.add("tbl")
    val objectElement1 = new SchemaElement
    objectElement1.label.add("organisation")
    schemaElement1.neighbors.put("worksfor", objectElement1)
    schemaElement1.neighbors.put("name", new SchemaElement)
    schemaElement1.neighbors.put("livesin", new SchemaElement)


    val schemaElement2 = new SchemaElement
    schemaElement2.label.add("organisation")
    schemaElement2.instances.add("zbw")
    schemaElement2.payload.add("http://zbw.eu")
    schemaElement2.neighbors.put("name", new SchemaElement)

    val goldElements = List(schemaElement1, schemaElement2)


    //parse n-triple file to RDD of GraphX Edges
    val edges = sc.textFile(testFile).filter(line => !line.isBlank).map(line => NTripleParser.parse(line))
    //build graph from vertices and edges from edges
    val graph: Graph[Set[(String, String)], (String, String, String, String)] = RDFGraphParser.parse(edges)

    val schemaExtraction: SchemaExtraction = SE_SchemEX

    /*
    Schema Summarization:
     */
    val schemaElements: VertexRDD[SchemaElement] = graph.aggregateMessages[SchemaElement](
      triplet => schemaExtraction.sendMessage(triplet),
      (a, b) => schemaExtraction.mergeMessage(a, b))

    assert(goldElements.size == schemaElements.count)

    schemaElements.collect().foreach(SE => {
      var foundMatch = false
      goldElements.foreach(SE_gold => {
        if(SE_gold.getID().equals(SE._2.getID()))
          foundMatch = true
      })
      assert(foundMatch)
    })

  }
}
