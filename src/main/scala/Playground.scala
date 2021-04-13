import input.{NTripleParser, RDFGraphParser}
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
    RDFGraphParser.classSignal = "type"
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
      println(vertexSummary)
      vertexSummary
    })

    println(initialGraph.vertices.collect.mkString("\n"))

    /**
     * Execute a Pregel-like iterative vertex-parallel abstraction. The user-defined vertex-program vprog is executed in parallel on each vertex receiving any inbound messages and computing a new value for the vertex. The sendMsg function is then invoked on all out-edges and is used to compute an optional message to the destination vertex. The mergeMsg function is a commutative associative function used to combine messages destined to the same vertex.
     * On the first iteration all vertices receive the initialMsg and on subsequent iterations if a vertex does not receive a message then the vertex-program is not invoked.
     * This function iterates until there are no remaining messages, or for maxIterations iterations.
     *
     * VD the vertex data type
     * ED the edge data type
     * A the Pregel message type
     * graph the input graph.
     * initialMsg the message each vertex will receive at the first iteration
     * maxIterations the maximum number of iterations to run for
     * activeDirection the direction of edges incident to a vertex that received a message in the previous round on which to run sendMsg. For example, if this is EdgeDirection.Out, only out-edges of vertices that received a message in the previous round will run. The default is EdgeDirection.Either, which will run sendMsg on edges where either side received a message in the previous round. If this is EdgeDirection.Both, sendMsg will only run on edges where *both* vertices received a message.
     * vprog the user-defined vertex program which runs on each vertex and receives the inbound message and computes a new vertex value. On the first iteration the vertex program is invoked on all vertices and is passed the default message. On subsequent iterations the vertex program is only invoked on those vertices that receive messages.
     * sendMsg a user supplied function that is applied to out edges of vertices that received messages in the current iteration
     * mergeMsg a user supplied function that takes two incoming messages of type A and merges them into a single message of type A. This function must be commutative and associative and ideally the size of A should not increase.
     * returns the resulting graph at the end of the computation
     */
    val sssp = initialGraph.pregel(new SchemaElement, 1, EdgeDirection.Out)(
      (id, oldVS, newVS) => oldVS.neighbor_update(oldVS, newVS), // Vertex Program
      triplet => SE_ComplexAttributeClassCollectionBisim.sendMessage(triplet),
      (a, b) => a.static_merge(a, b)
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
