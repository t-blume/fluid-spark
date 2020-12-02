import input.{NTripleParser, RDFGraphParser}
import org.apache.parquet.format.SchemaElement
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.{SparkConf, SparkContext}
import schema.{SE_ComplexAttributeClassCollectionBisim, SchemaElement}

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

    val newEdgesFile = "resources/manual-test-bisim.nq"

    val inputEdges = sc.textFile(newEdgesFile).filter(line => !line.trim.isEmpty).map(line => NTripleParser.parse(line))
    //build graph from vertices and edges from edges
    val graph = RDFGraphParser.parse(inputEdges)


    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, labelSet) => {
      //get origin types
      val vertexSummary = new SchemaElement
      if (labelSet != null)
        for ((t, p) <- labelSet) {
          vertexSummary.label.add(t)
          vertexSummary.payload.add(p)
        }

      (id, vertexSummary)
    })


    val sssp = initialGraph.pregel(new SchemaElement, 10, EdgeDirection.Out)(
      (id, oldVS, newVS) => SchemaElement.._2.merge(newVS), // Vertex Program
      triplet => SE_ComplexAttributeClassCollectionBisim.sendMessage(triplet),
      (a, b) => SE_ComplexAttributeClassCollectionBisim.mergeMessage(a, b)

    )
    //    val sssp = graph.pregel(Double.PositiveInfinity)(
    //      (id, oldVS, newVS) => oldVS._2.merge(newVS), // Vertex Program
    //      triplet => {  // Send Message
    //        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
    //          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
    //        } else {
    //          Iterator.empty
    //        }
    //      },
    //      (a, b) => math.min(a, b) // Merge Message
    //    )
    println(sssp.vertices.collect.mkString("\n"))
  }

}
