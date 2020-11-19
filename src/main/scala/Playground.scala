import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.graphx.util.GraphGenerators

object Playground {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PlaygroundPipeline").
      setMaster("local[*]").
      set("spark.driver.memory", "8g").
      set("spark.executor.memory", "8g").
      set("spark.core.max", "4").
      set("spark.executor.core", "4").
      set("spark.driver.maxResultSize", "0").
      set("spark.sparkContext.setCheckpointDir", ".")

    val sc = new SparkContext(conf)


    // A graph with edge attributes containing distances
    val graph: Graph[Long, Double] =
      GraphGenerators.logNormalGraph(sc, numVertices = 100).mapEdges(e => e.attr.toDouble)
    val sourceId: VertexId = 42 // The ultimate source
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) =>
      if (id == sourceId) 0.0 else Double.PositiveInfinity)

    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {  // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
    println(sssp.vertices.collect.mkString("\n"))
  }

}
