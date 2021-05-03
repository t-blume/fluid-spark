package schema

import junit.framework.TestCase

class VertexSummaryTest  extends TestCase {

  def testEmptySchemaElement(): Unit = {
    val schemaElement1 = new VertexSummaryOLD
    val schemaElement2 = new VertexSummaryOLD

    assert(schemaElement1.getID().equals(schemaElement2.getID()))

  }


  def testOrderSchemaElement(): Unit = {
    val schemaElement1 = new VertexSummaryOLD
    schemaElement1.neighbors.put("worksfor", new VertexSummaryOLD)
    schemaElement1.neighbors.put("name", new VertexSummaryOLD)
    schemaElement1.neighbors.put("livesin", new VertexSummaryOLD)

    val schemaElement2 = new VertexSummaryOLD
    schemaElement2.neighbors.put("worksfor", new VertexSummaryOLD)
    schemaElement2.neighbors.put("name", new VertexSummaryOLD)
    schemaElement2.neighbors.put("livesin", new VertexSummaryOLD)


    assert(schemaElement1.getID().equals(schemaElement2.getID()))


    val schemaElement3 = new VertexSummaryOLD
    schemaElement3.neighbors.put("livesin", new VertexSummaryOLD)
    schemaElement3.neighbors.put("name", new VertexSummaryOLD)
    schemaElement3.neighbors.put("worksfor", new VertexSummaryOLD)
    assert(schemaElement1.getID().equals(schemaElement3.getID()))

  }


  def testPayloadForSchema(): Unit = {
    val schemaElement1 = new VertexSummaryOLD
    schemaElement1.neighbors.put("worksfor", new VertexSummaryOLD)
    schemaElement1.neighbors.put("name", new VertexSummaryOLD)
    schemaElement1.neighbors.put("livesin", new VertexSummaryOLD)
    schemaElement1.payload.add("this payload")

    val schemaElement2 = new VertexSummaryOLD
    schemaElement2.neighbors.put("worksfor", new VertexSummaryOLD)
    schemaElement2.neighbors.put("name", new VertexSummaryOLD)
    schemaElement2.neighbors.put("livesin", new VertexSummaryOLD)
    schemaElement2.payload.add("a different payload")

    assert(schemaElement1.getID().equals(schemaElement2.getID()))
  }


  def testSummarizedInstanceForSchema(): Unit = {
    val schemaElement1 = new VertexSummaryOLD
    schemaElement1.neighbors.put("worksfor", new VertexSummaryOLD)
    schemaElement1.neighbors.put("name", new VertexSummaryOLD)
    schemaElement1.neighbors.put("livesin", new VertexSummaryOLD)
    schemaElement1.instances.add("this instance")

    val schemaElement2 = new VertexSummaryOLD
    schemaElement2.neighbors.put("worksfor", new VertexSummaryOLD)
    schemaElement2.neighbors.put("name", new VertexSummaryOLD)
    schemaElement2.neighbors.put("livesin", new VertexSummaryOLD)
    schemaElement2.instances.add("a different instance")

    assert(schemaElement1.getID().equals(schemaElement2.getID()))
  }
}
