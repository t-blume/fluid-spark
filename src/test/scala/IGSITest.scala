import java.util

import com.tinkerpop.blueprints.Vertex
import database.Constants.{PROPERTY_PAYLOAD, PROPERTY_SCHEMA_HASH}
import database.{MyConfig, OrientDb}
import junit.framework.TestCase

class IGSITest extends TestCase {


  def testAdd(): Unit = {

    val pipeline_batch1: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/schemex-test-1.conf"))
    pipeline_batch1.start()

    val pipeline_inc2: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/schemex-test-2.conf"))
    pipeline_inc2.start()

    val pipeline_batch2: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/schemex-test-2_gold.conf"))
    pipeline_batch2.start()

    val iterator_vertices_batch2: util.Iterator[Vertex] = OrientDb.getInstance(pipeline_batch2.database).graph.getVertices().iterator

    while (iterator_vertices_batch2.hasNext){
      val batchVertex = iterator_vertices_batch2.next()
      val incVertex = OrientDb.getInstance(pipeline_inc2.database).getVertexByHashID(PROPERTY_SCHEMA_HASH, batchVertex.getProperty(PROPERTY_SCHEMA_HASH))

      assert(incVertex != null)
      println(batchVertex.getProperty(PROPERTY_PAYLOAD))
      println(incVertex.getProperty(PROPERTY_PAYLOAD))
      assert(batchVertex.getProperty(PROPERTY_PAYLOAD).equals(incVertex.getProperty(PROPERTY_PAYLOAD)))
    }

  }



  //next iteration
  def testAdd_2(): Unit = {

    val pipeline_inc3: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/schemex-test-3.conf"))
    pipeline_inc3.start()

    val pipeline_batch3: ConfigPipeline = new ConfigPipeline(new MyConfig("resources/configs/schemex-test-3_gold.conf"))
    pipeline_batch3.start()

    val iterator_vertices_batch3: util.Iterator[Vertex] = OrientDb.getInstance(pipeline_batch3.database).graph.getVertices().iterator

    while (iterator_vertices_batch3.hasNext){
      val batchVertex = iterator_vertices_batch3.next()
      val incVertex = OrientDb.getInstance(pipeline_inc3.database).getVertexByHashID(PROPERTY_SCHEMA_HASH, batchVertex.getProperty(PROPERTY_SCHEMA_HASH))

      assert(incVertex != null)
      println("SE: " + batchVertex.getProperty(PROPERTY_SCHEMA_HASH))
      println("payGold: " + batchVertex.getProperty(PROPERTY_PAYLOAD))
      println("payInc: " + incVertex.getProperty(PROPERTY_PAYLOAD))
      assert(batchVertex.getProperty(PROPERTY_PAYLOAD).equals(incVertex.getProperty(PROPERTY_PAYLOAD)))
    }


  }


}
