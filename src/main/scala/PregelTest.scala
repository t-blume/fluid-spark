import Playground.{appName, maxCoresInt}
import org.apache.spark.graphx.{Edge, EdgeDirection}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PregelTest {

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

    val users: RDD[(VertexId, Int)] =
    sc.parallelize(Array((1L, 1), (2L, 1), (3L, 1), (4L, 1), (5L, 1)))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(1L, 2L, "collab"), Edge(1L, 3L, "advisor"),
        Edge(3L, 4L, "colleague"), Edge(4L, 5L, "pi")))


    // Build the initial Graph
    val graph = Graph(users, relationships)

    println(graph.vertices.collect.mkString("\n"))

    // Initialize the graph such that all vertices have random weights [0,10]
    //val initialGraph = graph.mapVertices((id, e) => 1.0)

    val sssp = graph.pregel(0, 1, EdgeDirection.In)(
      (id, weight, newWeight) => weight + newWeight, // Vertex Program. what to do with the new information
      triplet => { // Send Message: what to send to the neighbors
        Iterator((triplet.srcId, triplet.dstAttr))
      },
      (a, b) => a + b // Merge Message: aggregate all messages before you do something with it
    )
    println(sssp.vertices.collect.mkString("\n"))
  }

}
