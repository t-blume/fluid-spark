import java.util

import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import com.tinkerpop.blueprints.{Edge, Vertex}
import database.Constants.PROPERTY_SCHEMA_HASH
import database.{MyConfig, OrientConnector}
import junit.framework.TestCase


/**
 * Runs a series of tests to verify that the incremental index yields the same results as the batch computed index.
 * Note: make sure OrientDB is running before starting the tests.
 *
 * @author Till Blume, 13.02.2020
 */
class IGSITest extends TestCase {

  def testAdd(): Unit = {
    val pipeline_inc: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/manual-test-1.conf"))
    pipeline_inc.start()
    val pipeline_batch: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/manual-test-1_gold.conf"))
    pipeline_batch.start()
    validate(pipeline_inc, pipeline_batch)
  }



  //next iteration
  def testDelete(): Unit = {
    val pipeline_inc: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/manual-test-2.conf"))
    pipeline_inc.start()
    val pipeline_batch: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/manual-test-2_gold.conf"))
    pipeline_batch.start()
    validate(pipeline_inc, pipeline_batch)
  }

  //next iteration
  def testModification(): Unit = {
    val pipeline_inc: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/manual-test-3.conf"))
    pipeline_inc.start()
    val pipeline_batch: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/manual-test-3_gold.conf"))
    pipeline_batch.start()
    validate(pipeline_inc, pipeline_batch)
  }

  def testBatch(): Unit = {
    val pipeline_inc: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/timbl-test.conf"))
    pipeline_inc.start()
  }

//  def testScalability(): Unit = {
//    val pipeline_inc: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/scale-test.conf"))
//    pipeline_inc.start()
//    val pipeline_batch: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/scale-test_gold.conf"))
//    pipeline_batch.start()
//    validate(pipeline_inc, pipeline_batch)
//  }



  def validate(pipelineInc: ConfigPipeline, pipelineBatch: ConfigPipeline, debug: Boolean = true) {
    println("Comparing " + pipelineBatch.database + "_batch" + " and " + pipelineInc.database)
    val orientDbBatch: OrientConnector = OrientConnector.getInstance(pipelineBatch.database + "_batch", false, false, 1)

    val orientDbInc: OrientConnector = OrientConnector.getInstance(pipelineInc.database, false, false, 1)
    val verticesInc = orientDbInc.getGraph().countVertices
    val edgesInc = orientDbInc.getGraph().countEdges

    val verticesBatch = orientDbBatch.getGraph().countVertices
    val edgesBatch = orientDbBatch.getGraph().countEdges


    val graphBatch: OrientGraphNoTx = orientDbBatch.getGraph();
    val graphInc: OrientGraphNoTx = orientDbInc.getGraph();
    val iterator_edges_inc: util.Iterator[Edge] = graphInc.getEdges.iterator
    graphInc.makeActive()
    while (iterator_edges_inc.hasNext) {
      val incEdge = iterator_edges_inc.next()
      //get vertex with same hash in other db
      graphBatch.makeActive()
      val batchEdge = orientDbBatch.getEdgeByHashID(PROPERTY_SCHEMA_HASH, incEdge.getProperty(PROPERTY_SCHEMA_HASH))._result

      // assert it exists
      if(debug && batchEdge == null)
        println("Missing edge hash: " + incEdge.getProperty(PROPERTY_SCHEMA_HASH))
      assert(batchEdge != null)
      graphInc.makeActive()
    }


    assert(verticesBatch == verticesInc)
    assert(edgesBatch == edgesInc)

//    val graphBatch: OrientGraphNoTx = orientDbBatch.getGraph();
//    val graphInc: OrientGraphNoTx = orientDbInc.getGraph();
    val iterator_vertices_batch: util.Iterator[Vertex] = graphBatch.getVertices.iterator
    graphBatch.makeActive()
    while (iterator_vertices_batch.hasNext) {
      val batchVertex = iterator_vertices_batch.next()
      //get vertex with same hash in other db
      graphInc.makeActive()
      val incVertex = orientDbInc.getVertexByHashID(PROPERTY_SCHEMA_HASH, batchVertex.getProperty(PROPERTY_SCHEMA_HASH))._result

      // assert it exists
      assert(incVertex != null)
      val batchHash: Int = batchVertex.getProperty(PROPERTY_SCHEMA_HASH)
      val incHash: Int = incVertex.getProperty(PROPERTY_SCHEMA_HASH)

      graphBatch.makeActive()
      val batchPayload = orientDbBatch.getPayloadOfSchemaElement(batchHash)
      graphInc.makeActive()
      val incPayload = orientDbInc.getPayloadOfSchemaElement(incHash)
      //assert that the payload is equal
//      println("Batch: " + batchPayload)
//      println("Inc: " + incPayload)
      if (batchPayload == null)
        assert(incPayload == null)
      else
        assert(batchPayload.equals(incPayload))
      graphBatch.makeActive()
    }

    orientDbBatch.close()
    orientDbInc.close()
  }
}
