import _root_.database.{IGSI, OrientConnector, VertexUpdateHashIndex}
import input.{NTripleParser, RDFGraphParser}
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.{SparkConf, SparkContext}
import schema.VertexSummary
import utils.MyHash

import java.io.{File, FileOutputStream, PrintStream, PrintWriter}

object BisimTestPipeline {

  val appName = "BisimTestPipeline"

  val database = "bisim-test"

  val exportGraphSummaryDir = "exports/" + appName

  val secondaryIndexFile = "playSecondaryIndex.ser.gz"

  val chainingParameterK = 3

  if (exportGraphSummaryDir != null) {
    val file: File = new File(exportGraphSummaryDir)
    if (!file.exists) file.mkdirs
  }

  val maxCoresInt = 1


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(appName).
      setMaster("local[*]").
      set("spark.driver.memory", "8g").
      set("spark.executor.memory", "8g").
      set("spark.core.max", String.valueOf(maxCoresInt)).
      set("spark.executor.core", String.valueOf(maxCoresInt)).
      set("spark.driver.maxResultSize", "0").
      set("spark.sparkContext.setCheckpointDir", ".")


    val sc = new SparkContext(conf)
    val igsi = new IGSI(database, false, false)

    OrientConnector.create(database, true)
    OrientConnector.getInstance(database, false, false, maxCoresInt).
      setSecondaryIndex(VertexUpdateHashIndex.
        instantiate(false, false, false, secondaryIndexFile, false))

    val newEdgesFile = "resources/manual-test-bisim_new.nq"

    val inputEdges = sc.textFile(newEdgesFile).filter(line => !line.trim.isEmpty).map(line => NTripleParser.parse(line))
    RDFGraphParser.classSignal = "type"
    //build graph from vertices and edges from edges
    val graph = RDFGraphParser.parse(inputEdges)



    println(graph.vertices.collect.mkString("\n"))

    val initialGraph = graph.mapVertices((id, labelSet) => {
      // {(predicate, TypeSet)}
      val primaryVS = new VertexSummary()
      primaryVS.label = labelSet.map(elem => elem._1)
      //TODO: primaryVS.payload = labelSet.map(elem => elem._2)
      primaryVS.instances.add(String.valueOf(id))
      Map[String, VertexSummary]("_self" -> primaryVS)
    })

    println(initialGraph.vertices.collect.mkString("\n"))

    // Initialize the graph such that all vertices have random weights [0,10]
    //val initialGraph = graph.mapVertices((id, e) => 1.0)

    val sssp = initialGraph.pregel(Map[String, VertexSummary](), chainingParameterK, EdgeDirection.In)(
      (id, a, b) => {
        println("VP (merge): " + a + " and " + b + " -> result: " + (a ++ b))
        a ++ b
      }, // Vertex Program
      triplet => { // Send Message
        System.out.println("sndMsg: " + triplet)
        //build a secondary VS out of this primary VS (elevate relationships)
        val secondaryVS = new VertexSummary()
        secondaryVS.label = triplet.dstAttr.get("_self").get.label
        triplet.dstAttr.foreach(elem => {
          if (elem._1 != "_self"){
            secondaryVS.neighbors += elem
          }
        })
        // send secondary VS to source vertex
        Iterator((triplet.srcId, Map[String, VertexSummary](triplet.attr._2 -> secondaryVS)))
      },
      (a, b) => {
        //collect all incoming messages
        a ++ b
      }// Merge Message
    )
    println(sssp.vertices.collect.mkString("\n"))


    val primaryVertexSummaries = sssp.vertices.map(vertexMap => {
      val primaryVS = new VertexSummary()
      primaryVS.label = vertexMap._2.get("_self").get.label
      vertexMap._2.foreach(elem => {
        if (elem._1 != "_self"){
          primaryVS.neighbors += elem
        }
      })
      primaryVS.instances = vertexMap._2.get("_self").get.instances
      primaryVS
    })



    igsi.saveRDD(primaryVertexSummaries, (x: Iterator[VertexSummary]) => x, false, false, maxCoresInt)



    val file = new File(exportGraphSummaryDir + "/data-vertex-hash.csv")
    val pw = new PrintWriter(file)
    pw.write("Data Graph Vertex ID,hash\n")

    val src_ids = graph.edges.flatMap(edge => Set(edge.attr._1, edge.attr._3)).distinct.collect
    src_ids.foreach(src_id => {
      //pw.write(String.format("%s,%d\n", src_id, MyHash.hashString(src_id)))
      pw.write(src_id + "," + MyHash.hashString(src_id) + "\n")
    })
    pw.close()

    OrientConnector.getInstance(database, false, false, maxCoresInt).
      getSecondaryIndex.`export`(exportGraphSummaryDir + "/graph-summary-to-data-graph-mapping.csv")

    val ps = new PrintStream(new FileOutputStream(exportGraphSummaryDir + "/graph-summary.nt"))
    OrientConnector.getInstance(database, false, false, maxCoresInt).
      exportGraphSummaryAsNTriples("", RDFGraphParser.classSignal, ps)
  }


  /**
   *
  val sc = new SparkContext(conf)

    val set1 = Set[String]("person")
    val set2 = Set[String]("company")
    val set3 = Set[String]("university")

    val users: RDD[(VertexId, Set[String])] =
    sc.parallelize(Array((1L, set1),
      (2L, set2),
      (3L, set1),
      (4L, set3),
      (5L, set3)))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(1L, 2L, "worksat"), Edge(1L, 3L, "advisor"),
        Edge(3L, 4L, "worksat"), Edge(4L, 5L, "collab")))

    // Build the initial Graph
    val graph = Graph(users, relationships)
   *
   */
}
