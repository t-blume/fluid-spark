import Playground.{appName, maxCoresInt}
import org.apache.spark.graphx.{Edge, EdgeDirection}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import schema.SE_ComplexAttributeClassCollectionBisim.{mergeMessage, vertex_program}
import schema.VertexSummaryOLD

object PregelTest2 {

  import org.apache.spark.graphx.{Graph, VertexId}

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
      //get origin types
      val primaryVS = new VertexSummaryOLD()
      if (labelSet != null)
        for (t <- labelSet) {
          primaryVS.label.add(t)
        }
      println("inital primaryVS: " + primaryVS)
      val selfMap = new java.util.HashMap[String, VertexSummaryOLD]()
      selfMap.put("_self", primaryVS)
      selfMap
    })

    println(initialGraph.vertices.collect.mkString("\n"))

    // Initialize the graph such that all vertices have random weights [0,10]
    //val initialGraph = graph.mapVertices((id, e) => 1.0)

    val sssp = initialGraph.pregel(new java.util.HashMap[String, VertexSummaryOLD], 3, EdgeDirection.In)(
      (id, a, b) => { vertex_program(a, b)}, // Vertex Program
      triplet => { // Send Message
        System.out.println("sndMsg: " + triplet)
        val thisMap = new java.util.HashMap[String, VertexSummaryOLD]()
        thisMap.put(triplet.attr, triplet.dstAttr.get("_self"))
        Iterator((triplet.srcId, thisMap))
//        triplet.srcAttr.inProp = triplet.attr
//        //triplet.srcAttr.neighbors.merge(triplet.attr, triplet.dstAttr, triplet.dstAttr.static_merge)
//        Iterator((triplet.dstId, triplet.srcAttr))
      },
      (a, b) => { mergeMessage(a, b)
      }// Merge Message
    )
    println(sssp.vertices.collect.mkString("\n"))
  }

}
