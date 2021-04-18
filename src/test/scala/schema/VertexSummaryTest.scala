package schema

import junit.framework.TestCase

class VertexSummaryTest  extends TestCase {

  def testEmptySchemaElement(): Unit = {
    val schemaElement1 = new VertexSummary
    val schemaElement2 = new VertexSummary

    assert(schemaElement1.getID().equals(schemaElement2.getID()))

  }


  def testOrderSchemaElement(): Unit = {
    val schemaElement1 = new VertexSummary
    schemaElement1.neighbors.put("worksfor", new VertexSummary)
    schemaElement1.neighbors.put("name", new VertexSummary)
    schemaElement1.neighbors.put("livesin", new VertexSummary)

    val schemaElement2 = new VertexSummary
    schemaElement2.neighbors.put("worksfor", new VertexSummary)
    schemaElement2.neighbors.put("name", new VertexSummary)
    schemaElement2.neighbors.put("livesin", new VertexSummary)


    assert(schemaElement1.getID().equals(schemaElement2.getID()))


    val schemaElement3 = new VertexSummary
    schemaElement3.neighbors.put("livesin", new VertexSummary)
    schemaElement3.neighbors.put("name", new VertexSummary)
    schemaElement3.neighbors.put("worksfor", new VertexSummary)
    assert(schemaElement1.getID().equals(schemaElement3.getID()))

  }


  def testPayloadForSchema(): Unit = {
    val schemaElement1 = new VertexSummary
    schemaElement1.neighbors.put("worksfor", new VertexSummary)
    schemaElement1.neighbors.put("name", new VertexSummary)
    schemaElement1.neighbors.put("livesin", new VertexSummary)
    schemaElement1.payload.add("this payload")

    val schemaElement2 = new VertexSummary
    schemaElement2.neighbors.put("worksfor", new VertexSummary)
    schemaElement2.neighbors.put("name", new VertexSummary)
    schemaElement2.neighbors.put("livesin", new VertexSummary)
    schemaElement2.payload.add("a different payload")

    assert(schemaElement1.getID().equals(schemaElement2.getID()))
  }


  def testSummarizedInstanceForSchema(): Unit = {
    val schemaElement1 = new VertexSummary
    schemaElement1.neighbors.put("worksfor", new VertexSummary)
    schemaElement1.neighbors.put("name", new VertexSummary)
    schemaElement1.neighbors.put("livesin", new VertexSummary)
    schemaElement1.instances.add("this instance")

    val schemaElement2 = new VertexSummary
    schemaElement2.neighbors.put("worksfor", new VertexSummary)
    schemaElement2.neighbors.put("name", new VertexSummary)
    schemaElement2.neighbors.put("livesin", new VertexSummary)
    schemaElement2.instances.add("a different instance")

    assert(schemaElement1.getID().equals(schemaElement2.getID()))
  }
}
