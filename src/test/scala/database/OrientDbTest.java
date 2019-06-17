package database;

import graph.Edge;
import junit.framework.TestCase;
import scala.Tuple2;
import schema.ISchemaElement;
import schema.TypesAndProperties;
import utils.RandomString;

import java.util.Random;

import static database.Constants.CLASS_SCHEMA_ELEMENT;

public class OrientDbTest extends TestCase {

    private OrientDb testInstance;

    private static ISchemaElement[] testElements;


    public void setUp() throws Exception {
        super.setUp();
        OrientDb.create("remote:localhost", "JUNIT-TEST", "root", "rootpwd", true);

        testInstance = OrientDb.getInstance();
        int size = 10;

        testElements = new ISchemaElement[size];
        for (int i = 0; i < size; i++)
            testElements[i] = generateRandomTestInstance();

    }

    public void tearDown() throws Exception {
        testInstance.close();
    }


    private static ISchemaElement generateRandomTestInstance() {
        Random randomNumber = new Random();
        RandomString randomLabel = new RandomString(10);
        RandomString randomID = new RandomString(1);
        RandomString randomSource = new RandomString(3);

        ISchemaElement schemaElement = new TypesAndProperties();
        int numberOfLabel = randomNumber.nextInt(10) + 1;

        for (int i = 0; i < numberOfLabel; i++)
            schemaElement.getLabel().add(randomLabel.nextString());

        int numberOfEdges = randomNumber.nextInt(10) + 1;
        String startNode = randomID.nextString();

        for (int i = 0; i < numberOfEdges; i++) {
            Edge instanceEdge = new Edge();
            instanceEdge.start = startNode;
            instanceEdge.end = randomID.nextString();
            instanceEdge.label = randomLabel.nextString();
            instanceEdge.source = randomSource.nextString();

            Edge schemaEdge = new Edge();
            schemaEdge.label = instanceEdge.label;

            schemaElement.getSchemaEdges().add(new Tuple2<>(instanceEdge, schemaEdge));
        }
        return schemaElement;
    }

    public void testDeleteSchemaElement() {
        Random randomNumber = new Random();
        int index = randomNumber.nextInt(testElements.length);

        ISchemaElement schemaElement = testElements[index];
        assertFalse(testInstance.exists(CLASS_SCHEMA_ELEMENT, schemaElement.getID()));
        testInstance.writeSchemaElementWithEdges(schemaElement);
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