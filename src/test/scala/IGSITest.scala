import java.util

import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import database.Constants.PROPERTY_SCHEMA_HASH
import database.{MyConfig, OrientConnector}
import junit.framework.TestCase


/**
  * TODO FIXME: some connection problems with OrientDB require to run each test separately
  *
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
  def testAdd_2(): Unit = {
    val pipeline_inc: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/manual-test-2.conf"))
    pipeline_inc.start()
    val pipeline_batch: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/manual-test-2_gold.conf"))
    pipeline_batch.start()
    validate(pipeline_inc, pipeline_batch)
  }

  //next iteration
  def testAdd_3(): Unit = {
    val pipeline_inc: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/manual-test-3.conf"))
    pipeline_inc.start()
    val pipeline_batch: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/manual-test-3_gold.conf"))
    pipeline_batch.start()
    validate(pipeline_inc, pipeline_batch)
  }


  def testMultiThreading(): Unit = {
    val pipeline_inc: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/scale-test.conf"))
    pipeline_inc.start()
    val pipeline_batch: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/scale-test_gold.conf"))
    pipeline_batch.start()
    validate(pipeline_inc, pipeline_batch)
  }

  def validate(pipelineInc: ConfigPipeline, pipelineBatch: ConfigPipeline){
    println("Comparing " + pipelineBatch.database + " and " + pipelineInc.database)
    val orientDbBatch: OrientConnector = OrientConnector.getInstance(pipelineBatch.database, false)

    val orientDbInc: OrientConnector = OrientConnector.getInstance(pipelineInc.database, false)
    val verticesInc = orientDbInc.getGraph().countVertices
    val edgesInc=  orientDbInc.getGraph().countEdges

    val verticesBatch =  orientDbBatch.getGraph().countVertices
    val edgesBatch =  orientDbBatch.getGraph().countEdges



    assert(verticesBatch == verticesInc)
    assert(edgesBatch == edgesInc)

    val graphBatch: OrientGraphNoTx  = orientDbBatch.getGraph();
    val graphInc: OrientGraphNoTx  = orientDbInc.getGraph();
    val iterator_vertices_batch: util.Iterator[Vertex] = graphBatch.getVertices.iterator
    graphBatch.makeActive()
    while (iterator_vertices_batch.hasNext) {
      val batchVertex = iterator_vertices_batch.next()
      //get vertex with same hash in other db
      graphInc.makeActive()
      val incVertex = orientDbInc.getVertexByHashID(PROPERTY_SCHEMA_HASH, batchVertex.getProperty(PROPERTY_SCHEMA_HASH))

      // assert it exists
      assert(incVertex != null)
      val batchHash: Int = batchVertex.getProperty(PROPERTY_SCHEMA_HASH)
      val incHash: Int = incVertex.getProperty(PROPERTY_SCHEMA_HASH)

      graphBatch.makeActive()
      val batchPayload = orientDbBatch.getPayloadOfSchemaElement(batchHash)
      graphInc.makeActive()
      val incPayload = orientDbInc.getPayloadOfSchemaElement(incHash)
      //assert that the payload is equal
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
