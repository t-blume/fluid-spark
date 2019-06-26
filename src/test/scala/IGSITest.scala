import java.util

import com.tinkerpop.blueprints.Vertex
import database.Constants.PROPERTY_SCHEMA_HASH
import database.{MyConfig, OrientDb}
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

  def validate(pipelineInc: ConfigPipeline, pipelineBatch: ConfigPipeline){
    val orientDbBatch: OrientDb = OrientDb.getInstance(pipelineBatch.database, false)

    val verticesBatch =  orientDbBatch._graph.countVertices
    val edgesBatch =  orientDbBatch._graph.countEdges
    val orientDbInc: OrientDb = OrientDb.getInstance(pipelineInc.database, false)
    val verticesInc = orientDbInc._graph.countVertices
    val edgesInc=  orientDbInc._graph.countEdges
    assert(verticesBatch == verticesInc)
    assert(edgesBatch == edgesInc)

    val iterator_vertices_batch: util.Iterator[Vertex] = orientDbBatch._graph.getVertices.iterator
    orientDbBatch._graph.makeActive()
    while (iterator_vertices_batch.hasNext) {
      val batchVertex = iterator_vertices_batch.next()
      //get vertex with same hash in other db
      orientDbInc._graph.makeActive()
      val incVertex = orientDbInc.getVertexByHashID(PROPERTY_SCHEMA_HASH, batchVertex.getProperty(PROPERTY_SCHEMA_HASH))

      // assert it exists
      assert(incVertex != null)
      val batchHash: Int = batchVertex.getProperty(PROPERTY_SCHEMA_HASH)
      val incHash: Int = incVertex.getProperty(PROPERTY_SCHEMA_HASH)

      orientDbBatch._graph.makeActive()
      val batchPayload = orientDbBatch.getPayloadOfSchemaElement(batchHash)
      orientDbInc._graph.makeActive()
      val incPayload = orientDbInc.getPayloadOfSchemaElement(incHash)
      //assert that the payload is equal
      if (batchPayload == null)
        assert(incPayload == null)
      else
        assert(batchPayload.equals(incPayload))
      orientDbBatch._graph.makeActive()
    }

    orientDbBatch.close()
    orientDbInc.close()
  }
}
