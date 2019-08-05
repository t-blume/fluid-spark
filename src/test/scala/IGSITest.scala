import java.util

import com.tinkerpop.blueprints.Vertex
import database.Constants.PROPERTY_SCHEMA_HASH
import database.{MyConfig, OrientDb, OrientDbOpt}
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
    val pipeline_inc: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/wikidata-test.conf"))
    pipeline_inc.start()
    val pipeline_batch: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/tests/wikidata-test_gold.conf"))
    pipeline_batch.start()
    validate(pipeline_inc, pipeline_batch)
  }

  def validate(pipelineInc: ConfigPipeline, pipelineBatch: ConfigPipeline){
    val orientDbBatch: OrientDbOpt = OrientDbOpt.getInstance(pipelineBatch.database, false)

    val verticesBatch =  orientDbBatch.getGraph().countVertices
    val edgesBatch =  orientDbBatch.getGraph().countEdges
    val orientDbInc: OrientDbOpt = OrientDbOpt.getInstance(pipelineInc.database, false)
    val verticesInc = orientDbInc.getGraph().countVertices
    val edgesInc=  orientDbInc.getGraph().countEdges
    assert(verticesBatch == verticesInc)
    assert(edgesBatch == edgesInc)

    val iterator_vertices_batch: util.Iterator[Vertex] = orientDbBatch.getGraph().getVertices.iterator
    orientDbBatch.getGraph().makeActive()
    while (iterator_vertices_batch.hasNext) {
      val batchVertex = iterator_vertices_batch.next()
      //get vertex with same hash in other db
      orientDbInc.getGraph().makeActive()
      val incVertex = orientDbInc.getVertexByHashID(PROPERTY_SCHEMA_HASH, batchVertex.getProperty(PROPERTY_SCHEMA_HASH))

      // assert it exists
      assert(incVertex != null)
      val batchHash: Int = batchVertex.getProperty(PROPERTY_SCHEMA_HASH)
      val incHash: Int = incVertex.getProperty(PROPERTY_SCHEMA_HASH)

      orientDbBatch.getGraph().makeActive()
      val batchPayload = orientDbBatch.getPayloadOfSchemaElement(batchHash)
      orientDbInc.getGraph().makeActive()
      val incPayload = orientDbInc.getPayloadOfSchemaElement(incHash)
      //assert that the payload is equal
      if (batchPayload == null)
        assert(incPayload == null)
      else
        assert(batchPayload.equals(incPayload))
      orientDbBatch.getGraph().makeActive()
    }

    orientDbBatch.close()
    orientDbInc.close()
  }
}
