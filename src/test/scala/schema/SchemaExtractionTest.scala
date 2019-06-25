package schema

import input.{NTripleParser, RDFGraphParser}
import junit.framework.TestCase
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

class SchemaExtractionTest extends TestCase {
  val testFileCorrectness = "resources/manual-test-1.nq"
  val testFileAggregation = "resources/timbl-500.nq"
  val sc = new SparkContext(new SparkConf().setAppName("SchemaExtractionTest").
    setMaster("local[4]"))


  def testExtractionSchemEX(): Unit = {
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
    val edges = sc.textFile(testFileCorrectness).filter(line => !line.isBlank).map(line => NTripleParser.parse(line))
    //build _graph from vertices and edges from edges
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


  def testAggregationSchemEX(): Unit = {

    //parse n-triple file to RDD of GraphX Edges
    val edges = sc.textFile(testFileAggregation).filter(line => !line.isBlank).map(line => NTripleParser.parse(line))
    //build _graph from vertices and edges from edges
    val graph: Graph[Set[(String, String)], (String, String, String, String)] = RDFGraphParser.parse(edges)

    val schemaExtraction: SchemaExtraction = SE_SchemEX

    val schemaElements: VertexRDD[SchemaElement] = graph.aggregateMessages[SchemaElement](
      triplet => schemaExtraction.sendMessage(triplet),
      (a, b) => schemaExtraction.mergeMessage(a, b))


    schemaElements.map(x => (x._2.getID, mutable.HashSet(x._2))).reduceByKey(_ ++ _).collect().
      foreach(f = tuple => {
        tuple._2.foreach(SE => {
          //in ine aggregated set are only schema elements with the same hash / same schema
          assert(tuple._1 == SE.getID())
          //each aggregated schema element belongs to exactly one instance
          assert(SE.instances.size() == 1)
          //each schema element has payload
          assert(SE.payload.size() > 0)
        })
      })

  }
}
