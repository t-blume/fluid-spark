
import java.io.File

import database.OrientDb
import graph.Edge
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexRDD}
import schema.{ISchemaElement, SchemEX}

object TestingGrounds {
  def main(args: Array[String]) {

    //    create tmp directory
    val logDir: File = new File("/tmp/spark-events")
    if(!logDir.exists())
      logDir.mkdirs();

    //delete output directory
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[1]").set("spark.eventLog.enabled", "true").set("spark.eventLog.dir", "/tmp/spark-events")
    val sc = new SparkContext(conf)

    val inputFile = "resources/timbl-500.nq"
    val graphSilo: GraphSiloScala= new GraphSiloScala()
    val igsi: IGSIScalaNew = new IGSIScalaNew()

//
//    //OUT
    OrientDb.create("remote:localhost", "timbl-test-new", "root", "rootpwd", true)

    //parse n-triple file to RDD of GraphX Edges
    val edges = sc.textFile(inputFile).map(line => NTripleParser.parse(line))
    //build graph from vertices and edges from edges
    val graph: Graph[Set[(String, String)], (String, String, String, String)] = RDFGraphParser.parse(edges);




    val schemaElements: VertexRDD[ISchemaElement] = graph.aggregateMessages[ISchemaElement](
      triplet => { // Map Function
        // Send message to destination vertex containing types and property
        val srcElement = new SchemEX
        val dstElement = new SchemEX

        //get origin types
        if (triplet.srcAttr != null)
          for ((a, _) <- triplet.srcAttr)
            srcElement.getLabel().add(a)

        //get dst types
        if (triplet.dstAttr != null)
          for ((a, _) <- triplet.dstAttr)
            dstElement.getLabel().add(a)

        val edge: Edge = new Edge
        edge.start = triplet.attr._1
        edge.label = triplet.attr._2
        edge.end = triplet.attr._3
        edge.source = triplet.attr._4

        val schemaEdge: Edge = new Edge
        schemaEdge.start = String.valueOf(srcElement.getID)
        schemaEdge.label = edge.label
        schemaEdge.end = String.valueOf(dstElement.getID)

        srcElement.getPayload().add(edge.source)

        srcElement.getSchemaEdges.add(new Tuple2[Edge, Edge](edge, schemaEdge))
        triplet.sendToSrc(srcElement)
      },
      // Add counter and age

      (a, b) => {
        a.merge(b)
        a
      } // Reduce Function
    )

//    schemaElements.collect().foreach(S => println(S))
  //  schemaElements.map(x => (x._2.getID, Set(x._2))).reduceByKey(_ ++ _).collect().foreach(tuple => (igsi.tryAdd(tuple._2)))




    //    graph.triplets.foreach(triplet => println(s"<${triplet.srcId}(${triplet.srcAttr})> <${triplet.attr._2}> <${triplet.dstId}>"))


    // Output object property triples (${triplet.srcAttr._1})
    //graph.triplets.foreach(triplet => println(s"<${triplet.srcId}(${triplet.srcAttr})> <${triplet.attr._2}> <${triplet.dstId}>"))

    //    graph.edges.foreach(t => println(
    //            s"<${t.attr._1}> <${t.attr._2}> <${t.attr._3}> <${t.attr._4}>."
    //          ))
    //    graph.triplets.foreach(t => println(
    //      s"<${t.}> <${t.attr}> <${t.dstAttr}> ."
    //    ))

    //    // Output literal property triples
    //    users.foreach(t => println(
    //      s"""<$baseURI${t._2._1}> <${baseURI}role> \"${t._2._2}\" ."""
    //    ))

    sc.stop

  }


}
