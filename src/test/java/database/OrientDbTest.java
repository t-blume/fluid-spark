//package database;
//
//import junit.framework.TestCase;
//import schema.SchemaElement;
//
//import java.util.HashSet;
//import java.util.Random;
//
//public class OrientDbTest extends TestCase {
//
//    private OrientDbOptwithMem testInstance;
//
//    private static SchemaElement[] testElements;
//
//
//    public void setUp() throws Exception {
//        super.setUp();
//        OrientDbOptwithMem.create("JUNIT-TEST", true);
//
//        testInstance = OrientDbOptwithMem.getInstance("JUNIT-TEST", true);
//        int size = 20;
//
//        testElements = new SchemaElement[size];
//        for (int i = 0; i < size; i++)
//            testElements[i] = generateRandomTestInstance(0);
//
//    }
//
//    public void tearDown(){
//        testInstance.close();
//    }
//
//
//    /**
//     * generates a random instance an its schema element
//     *
//     * @param k chaining depth of schema
//     * @return
//     */
//    private static SchemaElement generateRandomTestInstance(int k) {
//        Random randomNumber = new Random();
////        RandomGraphGenerator randomLabel = new RandomGraphGenerator(10);
////        RandomGraphGenerator randomID = new RandomGraphGenerator(12);
////        RandomGraphGenerator randomSource = new RandomGraphGenerator(3);
////
////        SchemaElement schemaElement = new SchemaElement();
////        int numberOfLabel = randomNumber.nextInt(10) + 1;
////
////        for (int i = 0; i < numberOfLabel; i++)
////            schemaElement.label().add(randomLabel.nextString());
////
////        int numberOfPayload = randomNumber.nextInt(4) + 1;
////
////        for (int i = 0; i < numberOfPayload; i++)
////            schemaElement.payload().add(randomSource.nextString());
////
////        schemaElement.instances().add(randomID.nextString());
////
////        if (k > 0) {
////            int numberOfEdges = randomNumber.nextInt(10) + 1;
////            for (int i = 0; i < numberOfEdges; i++) {
//////                schemaElement.neighbors().put()
//////                instanceEdge.end = randomID.nextString();
//////                instanceEdge.label = randomLabel.nextString();
//////                instanceEdge.source = randomSource.nextString();
//////
//////                Edge schemaEdge = new Edge();
//////                schemaEdge.label = instanceEdge.label;
//////
//////                schemaElement.getSchemaEdges().add(new Tuple2<>(instanceEdge, schemaEdge));
////            }
////
////        }
//        return null;
//    }
//
//
//
//    public void testDeleteSchemaElement() {
////        Random randomNumber = new Random();
////        int index = randomNumber.nextInt(testElements.length);
////
////        SchemaElement schemaElement = testElements[index];
////        assertFalse(testInstance.exists(CLASS_SCHEMA_ELEMENT, schemaElement.getID()));
////        testInstance.writeOrUpdateSchemaElement(schemaElement, new HashSet<>(), true);
////        assertTrue(testInstance.exists(CLASS_SCHEMA_ELEMENT, schemaElement.getID()));
////        testInstance.deleteSchemaElement(schemaElement.getID());
////        assertFalse(testInstance.exists(CLASS_SCHEMA_ELEMENT, schemaElement.getID()));
//    }
//
//
//    public void testWriteSchemaElement() {
////        Random randomNumber = new Random();
////        int index = randomNumber.nextInt(testElements.length);
////
////        SchemaElement schemaElement = testElements[index];
////        assertFalse(testInstance.exists(CLASS_SCHEMA_ELEMENT, schemaElement.getID()));
////        testInstance.writeOrUpdateSchemaElement(schemaElement, new HashSet<>(), true);
////        assertTrue(testInstance.exists(CLASS_SCHEMA_ELEMENT, schemaElement.getID()));
////        testInstance.deleteSchemaElement(schemaElement.getID());
////        assertFalse(testInstance.exists(CLASS_SCHEMA_ELEMENT, schemaElement.getID()));
//
//    }
//
//    public void testDeletePayloadElement() {
//    }
//
//    public void testDeletePayloadEdge() {
//    }
//
//    public void testClose() {
//    }
//
//    public void testGetPreviousElementID() {
//    }
//
//    public void testRemoveNodeFromSchemaElement() {
//
//        Random randomNumber = new Random();
//        int index = randomNumber.nextInt(testElements.length);
//
//        SchemaElement schemaElement = testElements[index];
//        testInstance.writeOrUpdateSchemaElement(schemaElement, new HashSet<>(), true);
//        int instanceID = schemaElement.instances().iterator().next().hashCode();
//
////        testInstance.addNodeToSchemaElement(instanceID, schemaElement.getID(), schemaElement.payload());
////        assertTrue(testInstance.exists(CLASS_IMPRINT_VERTEX, instanceID));
////
////
////
////        assertTrue(testInstance.exists(CLASS_IMPRINT_RELATION, MyHash.md5HashImprintRelation(instanceID, schemaElement.getID())));
////        testInstance.deleteSchemaElement(schemaElement.getID());
////        assertFalse(testInstance.exists(CLASS_SCHEMA_ELEMENT, schemaElement.getID()));
//
//
//    }
//
//    public void testAddNodeFromSchemaElement() {
//    }
//
//    public void testGetPreviousLinkID() {
//    }
//
//    public void testRemoveEdgeFromSchemaEdge() {
//    }
//
//    public void testAddEdgeFromSchemaEdge() {
//    }
//
//
//}