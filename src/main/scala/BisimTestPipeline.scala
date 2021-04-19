import org.apache.spark.graphx.{Edge, EdgeDirection}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import schema.VertexSummaryNew
import org.apache.spark.graphx.{Graph, VertexId}
import java.io.File

object BisimTestPipeline {

  val appName = "BisimTestPipeline"

  val database = "bisim-test"

  val exportGraphSummaryDir = "exports/" + appName

  val secondaryIndexFile = "playSecondaryIndex.ser.gz"

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

    println(graph.vertices.collect.mkString("\n"))

    val initialGraph = graph.mapVertices((id, labelSet) => {
      // {(predicate, TypeSet)}
      val primaryVS = new VertexSummaryNew()
      primaryVS.label = labelSet
      Map[String, VertexSummaryNew]("_self" -> primaryVS)
    })

    println(initialGraph.vertices.collect.mkString("\n"))

    // Initialize the graph such that all vertices have random weights [0,10]
    //val initialGraph = graph.mapVertices((id, e) => 1.0)

    val sssp = initialGraph.pregel(Map[String, VertexSummaryNew](), 3, EdgeDirection.In)(
      (id, a, b) => {
        println("VP (merge): " + a + " and " + b + " -> result: " + (a ++ b))
        a ++ b
      }, // Vertex Program
      triplet => { // Send Message
        System.out.println("sndMsg: " + triplet)
        //build a secondary VS out of this primary VS (elevate relationships)
        val secondaryVS = new VertexSummaryNew()
        secondaryVS.label = triplet.dstAttr.get("_self").get.label
        triplet.dstAttr.foreach(elem => {
          if (elem._1 != "_self"){
            secondaryVS.neighbors += elem
          }
        })
        // send secondary VS to source vertex
        Iterator((triplet.srcId, Map[String, VertexSummaryNew](triplet.attr -> secondaryVS)))
      },
      (a, b) => {
        //collect all incoming messages
        a ++ b
      }// Merge Message
    )
    println(sssp.vertices.collect.mkString("\n"))
  }

}
