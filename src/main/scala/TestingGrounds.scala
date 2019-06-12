
import java.io.File

import database.OrientDb
import graph.Edge
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext}
import schema.{ISchemaElement, SchemEX}

import scala.collection.mutable

object TestingGrounds {
  def main(args: Array[String]) {

    //    create tmp directory
    val logDir: File = new File("/tmp/spark-events")
    if(!logDir.exists())
      logDir.mkdirs();

    //delete output directory
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[1]").set("spark.eventLog.enabled", "true").set("spark.eventLog.dir", "/tmp/spark-events")
    val sc = new SparkContext(conf)

    val inputFile = "resources/manual-test-1.nq"
    val graphSilo: GraphSiloScala= new GraphSiloScala()
    val igsi: IGSIScalaNew = new IGSIScalaNew()

//
//    //OUT
    OrientDb.create("remote:localhost", "till-test", "root", "rootpwd", true)

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

        schemaEdge.label = edge.label
        schemaEdge.end = if(dstElement.getLabel.size() > 0) String.valueOf(dstElement.getID) else null;

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
    schemaElements.map(x => (x._2.getID, mutable.HashSet(x._2))).reduceByKey(_ ++ _).collect().foreach(tuple => igsi.tryAdd(tuple._2))

    sc.stop

  }


}
