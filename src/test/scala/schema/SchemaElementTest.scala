package schema

import junit.framework.TestCase

class SchemaElementTest  extends TestCase {

  def testEmptySchemaElement(): Unit = {
    val schemaElement1 = new SchemaElement
    val schemaElement2 = new SchemaElement

    assert(schemaElement1.getID().equals(schemaElement2.getID()))

  }


  def testOrderSchemaElement(): Unit = {
    val schemaElement1 = new SchemaElement
    schemaElement1.neighbors.put("worksfor", new SchemaElement)
    schemaElement1.neighbors.put("name", new SchemaElement)
    schemaElement1.neighbors.put("livesin", new SchemaElement)

    val schemaElement2 = new SchemaElement
    schemaElement2.neighbors.put("worksfor", new SchemaElement)
    schemaElement2.neighbors.put("name", new SchemaElement)
    schemaElement2.neighbors.put("livesin", new SchemaElement)


    assert(schemaElement1.getID().equals(schemaElement2.getID()))


    val schemaElement3 = new SchemaElement
    schemaElement3.neighbors.put("livesin", new SchemaElement)
    schemaElement3.neighbors.put("name", new SchemaElement)
    schemaElement3.neighbors.put("worksfor", new SchemaElement)
    assert(schemaElement1.getID().equals(schemaElement3.getID()))

  }


  def testPayloadForSchema(): Unit = {
    val schemaElement1 = new SchemaElement
    schemaElement1.neighbors.put("worksfor", new SchemaElement)
    schemaElement1.neighbors.put("name", new SchemaElement)
    schemaElement1.neighbors.put("livesin", new SchemaElement)
    schemaElement1.payload.add("this payload")

    val schemaElement2 = new SchemaElement
    schemaElement2.neighbors.put("worksfor", new SchemaElement)
    schemaElement2.neighbors.put("name", new SchemaElement)
    schemaElement2.neighbors.put("livesin", new SchemaElement)
    schemaElement2.payload.add("a different payload")

    assert(schemaElement1.getID().equals(schemaElement2.getID()))
  }


  def testSummarizedInstanceForSchema(): Unit = {
    val schemaElement1 = new SchemaElement
    schemaElement1.neighbors.put("worksfor", new SchemaElement)
    schemaElement1.neighbors.put("name", new SchemaElement)
    schemaElement1.neighbors.put("livesin", new SchemaElement)
    schemaElement1.instances.add("this instance")

    val schemaElement2 = new SchemaElement
    schemaElement2.neighbors.put("worksfor", new SchemaElement)
    schemaElement2.neighbors.put("name", new SchemaElement)
    schemaElement2.neighbors.put("livesin", new SchemaElement)
    schemaElement2.instances.add("a different instance")

    assert(schemaElement1.getID().equals(schemaElement2.getID()))
  }
}
