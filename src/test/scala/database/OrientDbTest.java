package database;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import junit.framework.TestCase;
import schema.SchemaElement;
import utils.RandomString;

import java.util.Random;

import static database.Constants.CLASS_SCHEMA_ELEMENT;

public class OrientDbTest extends TestCase {

    private OrientDb testInstance;

    private static SchemaElement[] testElements;


    public void setUp() throws Exception {
//        super.setUp();
//        OrientDb.create("JUNIT-TEST", true);
//
//        testInstance = OrientDb.getInstance("JUNIT-TEST", true);
//        int size = 10;
//
//        testElements = new SchemaElement[size];
//        for (int i = 0; i < size; i++)
//            testElements[i] = generateRandomTestInstance(0);

    }

//    public void tearDown(){
//        testInstance.close();
//    }


    /**
     * generates a random instance an its schema element
     *
     * @param k chaining depth of schema
     * @return
     */
    private static SchemaElement generateRandomTestInstance(int k) {
        Random randomNumber = new Random();
        RandomString randomLabel = new RandomString(10);
        RandomString randomID = new RandomString(1);
        RandomString randomSource = new RandomString(3);

        SchemaElement schemaElement = new SchemaElement();
        int numberOfLabel = randomNumber.nextInt(10) + 1;

        for (int i = 0; i < numberOfLabel; i++)
            schemaElement.label().add(randomLabel.nextString());

        schemaElement.instances().add(randomID.nextString());

        if (k > 0) {
            int numberOfEdges = randomNumber.nextInt(10) + 1;
            for (int i = 0; i < numberOfEdges; i++) {
//                schemaElement.neighbors().put()
//                instanceEdge.end = randomID.nextString();
//                instanceEdge.label = randomLabel.nextString();
//                instanceEdge.source = randomSource.nextString();
//
//                Edge schemaEdge = new Edge();
//                schemaEdge.label = instanceEdge.label;
//
//                schemaElement.getSchemaEdges().add(new Tuple2<>(instanceEdge, schemaEdge));
            }

        }
        return schemaElement;
    }

    public void testMultiAccess() {
        ODatabaseSession session1 = OrientDb.getDBSession("till-test");
        session1.activateOnCurrentThread();
        ODatabaseSession session2 = OrientDb.getDBSession("till-test-gold");
        session2.activateOnCurrentThread();

        ORecordIteratorClass<ODocument> iterator1 = session1.browseClass(CLASS_SCHEMA_ELEMENT);

        while (iterator1.hasNext()) {
            ODocument batchVertex = iterator1.next();
            //get vertex with same hash in other db
            session2.getClusters();

        }
    }

    public void testDeleteSchemaElement() {
        Random randomNumber = new Random();
        int index = randomNumber.nextInt(testElements.length);

        SchemaElement schemaElement = testElements[index];
        assertFalse(testInstance.exists(CLASS_SCHEMA_ELEMENT, schemaElement.getID()));
        testInstance.writeOrUpdateSchemaElement(schemaElement);
        assertTrue(testInstance.exists(CLASS_SCHEMA_ELEMENT, schemaElement.getID()));
        testInstance.deleteSchemaElement(schemaElement.getID());
        assertFalse(testInstance.exists(CLASS_SCHEMA_ELEMENT, schemaElement.getID()));
    }

    public void testDeleteSchemaEdge() {
    }

    public void testExists() {
    }

    public void testWriteSchemaElementWithEdges() {
    }

    public void testDeletePayloadElement() {
    }

    public void testDeletePayloadEdge() {
    }

    public void testClose() {
    }

    public void testGetPreviousElementID() {
    }

    public void testRemoveNodeFromSchemaElement() {
    }

    public void testAddNodeFromSchemaElement() {
    }

    public void testGetPreviousLinkID() {
    }

    public void testRemoveEdgeFromSchemaEdge() {
    }

    public void testAddEdgeFromSchemaEdge() {
    }


}