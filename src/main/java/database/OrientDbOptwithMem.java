package database;

import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import scala.Serializable;
import schema.SchemaElement;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static database.Constants.*;

/**
 * NOTE from Tinkerpop:  Edge := outVertex ---label---> inVertex.
 */
public class OrientDbOptwithMem implements Serializable {

    public static final int MAX_RETRIES = 5;

    /************************************************
     defined once at start up for all dbs
     ************************************************/
    public static String URL = "remote:localhost";
    public static String USERNAME = "admin";
    public static String PASSWORD = "admin";
    public static String serverUser = "root";
    public static String serverPassword = "rootpwd";
    /************************************************/

    private static OrientGraphFactory factory = null;
    private static OrientDB databaseServer = null;
    private static OrientDbOptwithMem instance = null;
    private static ODatabasePool pool = null;

    public static OrientDbOptwithMem getInstance(String database, boolean trackChanges) {
        if (factory == null)
            factory = new OrientGraphFactory(URL + "/" + database).setupPool(1, 10);

        if (instance == null)
            instance = new OrientDbOptwithMem(database, trackChanges);

        return instance;
    }

//    private static ODatabaseSession getDBSession() {
//        // OPEN DATABASE
//
//    }

    /**
     * Creates the database if not existed before.
     * Optionally, cleared the content.
     *
     * @param database
     * @param clear
     */
    public static void create(String database, boolean clear) {
        databaseServer = new OrientDB(URL, serverUser, serverPassword, OrientDBConfig.defaultConfig());
        pool = new ODatabasePool(databaseServer, database, USERNAME, PASSWORD);
        if (databaseServer.exists(database) && clear)
            databaseServer.drop(database);

        if (!databaseServer.exists(database)) {
            databaseServer.create(database, ODatabaseType.PLOCAL);
            try (ODatabaseSession databaseSession = pool.acquire()) {
                //this is quite important to align this with the OS
                databaseSession.command("ALTER DATABASE TIMEZONE \"GMT+2\"");

                /*
                    Create Schema Elements
                 */
                databaseSession.createVertexClass(CLASS_SCHEMA_ELEMENT);
                databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createProperty(PROPERTY_SCHEMA_HASH, OType.INTEGER);
                databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createIndex(CLASS_SCHEMA_ELEMENT + "." + PROPERTY_SCHEMA_HASH, OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, PROPERTY_SCHEMA_HASH);
                databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createProperty(PROPERTY_SCHEMA_VALUES, OType.EMBEDDEDSET);
                databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createProperty(PROPERTY_SUMMARIZED_INSTANCES, OType.EMBEDDEDSET);

                /*
                Create relationships between schema elements
                 */
                databaseSession.createEdgeClass(CLASS_SCHEMA_RELATION);
                databaseSession.getClass(CLASS_SCHEMA_RELATION).createProperty(PROPERTY_SCHEMA_HASH, OType.INTEGER);

                /*
                Create super brain
                 */
                databaseSession.createClass(CLASS_IMPRINT_VERTEX);
                databaseSession.getClass(CLASS_IMPRINT_VERTEX).createProperty(PROPERTY_IMPRINT_ID, OType.INTEGER);
                databaseSession.getClass(CLASS_IMPRINT_VERTEX).createIndex(CLASS_IMPRINT_VERTEX + "." + PROPERTY_IMPRINT_ID, OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, PROPERTY_IMPRINT_ID);

                databaseSession.getClass(CLASS_IMPRINT_VERTEX).createProperty(PROPERTY_TIMESTAMP, OType.DATETIME);
                databaseSession.getClass(CLASS_IMPRINT_VERTEX).createProperty(PROPERTY_PAYLOAD, OType.EMBEDDEDSET);
                databaseSession.getClass(CLASS_IMPRINT_VERTEX).createProperty(PROPERTY_IMPRINT_RELATION, OType.INTEGER);

                databaseSession.commit();
            }
        }
    }


    /******************************************
     * Start of the OrientDB Connector object *
     ******************************************/

    //name of the database
    private String database;
    //keep track of all update operations
    public ChangeTracker _changeTracker = null;


    public OrientGraph getGraph() {
        return factory.getTx();
    }

    public OrientDbOptwithMem(String database, boolean trackChanges) {
        this.database = database;
        if (trackChanges)
            _changeTracker = new ChangeTracker();
    }


    /**
     * Deletes the schema element and all of its edges
     *
     * @param schemaHash
     */
    public void deleteSchemaElement(Integer schemaHash) {
        OrientGraph graph = factory.getTx();
        //use loop but is actually always one schema element!
        for (Vertex v : graph.getVertices(PROPERTY_SCHEMA_HASH, schemaHash)) {
            //check if there are incoming links to that schema element
            Iterator<Edge> edgeIterator = v.getEdges(Direction.OUT, CLASS_SCHEMA_RELATION).iterator();
            while (edgeIterator.hasNext()) {
                Edge edge = edgeIterator.next();
                Vertex linkedSchemaElement = edge.getVertex(Direction.IN);
                int remainingLinks = 0;
                Iterator<Edge> links = linkedSchemaElement.getEdges(Direction.IN).iterator();
                while (remainingLinks <= 2 && links.hasNext()) {
                    links.next();
                    remainingLinks++;
                }
                if (remainingLinks < 2) {
                    //when we remove this link, the linked schema element will be an orphan so remove it as well
                    deleteSchemaElement(linkedSchemaElement.getProperty(PROPERTY_SCHEMA_HASH));
                }
            }

            SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
            Set<Integer> summarizedInstances = secondaryIndex.removeSchemaElementLink(v.getProperty(PROPERTY_SCHEMA_HASH));
            if (summarizedInstances != null)
                secondaryIndex.removeImprintLinksByID(summarizedInstances);


            graph.removeVertex(v);
            if (_changeTracker != null)
                _changeTracker._schemaElementsDeleted++;
        }
        graph.commit();
        graph.shutdown();
    }


    public boolean exists(String classString, Integer hashValue) {
        boolean exists;
        if (classString == CLASS_SCHEMA_ELEMENT) {
            exists = getVertexByHashID(PROPERTY_SCHEMA_HASH, hashValue) != null;
        } else if (classString == CLASS_IMPRINT_VERTEX) {
            exists = getVertexByHashID(PROPERTY_IMPRINT_ID, hashValue) != null;
            return exists;
        } else if (classString == CLASS_IMPRINT_RELATION) {
            exists = getEdgeByHashID(PROPERTY_IMPRINT_ID, hashValue) != null;
        } else {
            System.err.println("Invalid exists-query!");
            return false;
        }
        return exists;
    }


    /**
     * CONVENTION:
     * the schema computation can return the following:
     * - no schema edges at all (k=0) => leads to no outgoing edges
     * - schema edges where we ignore the target (null) => leads to outgoing edges with an EMPTY TARGET placeholder
     * - schema edges where we take the schema of the neighbour into account
     * => write second-class schema element with no imprint vertex but shared among all first-class elements
     *
     * @param schemaElement
     */
    public void writeOrUpdateSchemaElement(SchemaElement schemaElement, Set<Integer> instances, boolean primary) {
        OrientGraph graph = factory.getTx();
        if (!exists(CLASS_SCHEMA_ELEMENT, schemaElement.getID())) {
            if (_changeTracker != null && primary)
                _changeTracker._newSchemaStructureObserved++;
            //create a new schema element
            Vertex vertex = graph.addVertex("class:" + CLASS_SCHEMA_ELEMENT);
            vertex.setProperty(PROPERTY_SCHEMA_HASH, schemaElement.getID());
            vertex.setProperty(PROPERTY_SCHEMA_VALUES, schemaElement.label());
            if (instances != null) {
                SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
                secondaryIndex.putSummarizedInstances(schemaElement.getID(), instances);
            }


            schemaElement.neighbors().forEach((K, V) -> {
                // if schema computation did not set a neighbor element, then use an empty one
                Integer endID = V == null ? EMPTY_SCHEMA_ELEMENT_HASH : V.getID();
                Vertex targetV = getVertexByHashID(PROPERTY_SCHEMA_HASH, endID);
                if (targetV == null) {
                    //This node does not yet exist, so create one
                    //NOTE: neighbor elements are second-class citizens that exist as long as another schema element references them
                    //NOTE: this is a recursive step depending on chaining parameterization k
                    writeOrUpdateSchemaElement(V == null ? new SchemaElement() : V, null, false);
                }
                targetV = getVertexByHashID(PROPERTY_SCHEMA_HASH, endID);
                Edge edge = graph.addEdge("class:" + CLASS_SCHEMA_RELATION, vertex, targetV, CLASS_SCHEMA_RELATION);
                edge.setProperty(PROPERTY_SCHEMA_VALUES, K);
            });
            if (_changeTracker != null)
                _changeTracker._schemaElementsAdded++;

            graph.shutdown();
        } else {
            SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
            secondaryIndex.addSummarizedInstances(schemaElement.getID(), instances);
        }
    }


    public Set<String> getPayloadOfSchemaElement(Integer schemaHash) {
        SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
        Set<Imprint> imprints = secondaryIndex.getSummarizedInstances(schemaHash);
        Set<String> payload = new HashSet<>();
        if (imprints != null) {
            for (Imprint imprint : imprints)
                if (imprint._payload != null)
                    payload.addAll(imprint._payload);
        }
        return payload;
    }


    /**
     * Closes connection to database
     */
    public void close() {
        //TODO save Memory Store here
    }


    /**
     * Get linked schema element hash from instance ID
     *
     * @param nodeID
     * @return
     */
    public Integer getPreviousElementID(Integer nodeID) {
        SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
        return secondaryIndex.getSchemaElementFromImprintID(nodeID);
    }

//    /**
//     * Get linked schema element from instance ID
//     *
//     * @param nodeID
//     * @return
//     */
//    public Vertex getPreviousElement(Integer nodeID) {
//        OrientGraph graph = factory.getTx();
//        Iterator<Vertex> iterator = graph.getVertices(CLASS_IMPRINT_VERTEX + "." + PROPERTY_IMPRINT_ID, nodeID).iterator();
//        if (iterator.hasNext()) {
//            Iterator<Edge> innerIterator = iterator.next().getEdges(Direction.OUT, CLASS_IMPRINT_RELATION).iterator();
//            if (innerIterator.hasNext()) {
//                Vertex v = innerIterator.next().getVertex(Direction.IN);
//                graph.shutdown();
//                return v;
//            }
//        }
//        return null;
//    }

    /**
     * Returns true if it also removed the schema element
     *
     * @param nodeID
     * @param schemaHash
     * @return
     */
    public boolean removeNodeFromSchemaElement(Integer nodeID, Integer schemaHash) {
        SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
        boolean deleteSchemaElement = secondaryIndex.removeSummarizedInstance(schemaHash, nodeID);
        if (_changeTracker != null)
            _changeTracker._removedInstanceToSchemaLinks++;

        if (deleteSchemaElement) {
            deleteSchemaElement(schemaHash);
            //no more instance with that schema exists
            if (_changeTracker != null)
                _changeTracker._schemaStructureDeleted++;
        }
        return deleteSchemaElement;
    }

    public void removeNodesFromSchemaElement(Map<Integer, Integer> nodes) {
        for (Map.Entry<Integer, Integer> node : nodes.entrySet()) {
            removeNodeFromSchemaElement(node.getValue(), node.getKey());
        }
    }

    public void addNodesToSchemaElement(Map<Integer, Set<String>> nodes, Integer schemaHash) {
        SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
        secondaryIndex.addNodesToSchemaElement(nodes, schemaHash);
    }

//    /**
//     * Update or create an imprint vertex and link it to the schema element and the payload
//     *
//     * @param nodeID
//     * @param schemaHash
//     * @param payload
//     * @return
//     */
//    public void addNodeToSchemaElement(Integer nodeID, Integer schemaHash, Set<String> payload) {
//        OrientGraphNoTx graph = factory.getNoTx();
//        // get or create the imprint vertex
//        Iterator<Vertex> iterator = graph.getVertices(CLASS_IMPRINT_VERTEX + "." + PROPERTY_IMPRINT_ID, nodeID).iterator();
//        Vertex imprint;
//        if (!iterator.hasNext()) {
//            imprint = graph.addVertex("class:" + CLASS_IMPRINT_VERTEX);
//            //unique reference to instance
//            imprint.setProperty(PROPERTY_IMPRINT_ID, nodeID);
//            //memorize which payload was extracted from this instance
//            imprint.setProperty(PROPERTY_PAYLOAD, updatePayload(new HashSet<>(), payload, true, false));
//            //set current time so avoid deletion after completion
//            imprint.setProperty(PROPERTY_TIMESTAMP, NOW());
//        } else {
//            imprint = iterator.next();
//            Set<String> oldPayload = imprint.getProperty(PROPERTY_PAYLOAD);
//            //handle payload updates here
//            if (oldPayload.hashCode() != payload.hashCode()) {
//                //the payload extracted from that instance has changed
//                //set the payload to to exactly new new one
//                imprint.setProperty(PROPERTY_PAYLOAD, updatePayload(oldPayload, payload, false, false));
//                //graph.commit();
//            }
//            //set current time so avoid deletion after completion
//            imprint.setProperty(PROPERTY_TIMESTAMP, NOW());
//        }
//
//        //get the schema element
//        iterator = graph.getVertices(CLASS_SCHEMA_ELEMENT + "." + PROPERTY_SCHEMA_HASH, schemaHash).iterator();
//        Vertex schema = iterator.next();
//
//        if (!exists(CLASS_IMPRINT_RELATION, MyHash.md5HashImprintRelation(nodeID, schemaHash))) {
//            Edge imprintEdge = graph.addEdge("class:" + CLASS_IMPRINT_RELATION, imprint, schema, CLASS_IMPRINT_RELATION);
//            imprintEdge.setProperty(PROPERTY_IMPRINT_ID, MyHash.md5HashImprintRelation(nodeID, schemaHash));
//        }
//        graph.commit();
//        graph.shutdown();
//    }


//    public void touch(Integer nodeID, Set<String> payload) {
//        OrientGraph graph = factory.getTx();
//
//        // get the imprint vertex
//        Iterator<Vertex> iterator = graph.getVertices(CLASS_IMPRINT_VERTEX + "." + PROPERTY_IMPRINT_ID, nodeID).iterator();
//        Vertex imprint;
//        if (!iterator.hasNext()) {
//            System.err.println("This should not happen!");
//        } else {
//            imprint = iterator.next();
//            imprint.setProperty(PROPERTY_TIMESTAMP, NOW());
//            Set<String> oldPayload = imprint.getProperty(PROPERTY_PAYLOAD);
//            if (oldPayload.hashCode() != payload.hashCode()) {
//                imprint.setProperty(PROPERTY_PAYLOAD, updatePayload(oldPayload, payload, false, false));
//            }
//        }
//        graph.commit();
//        graph.shutdown();
//    }

    public void touchMultiple(Map<Integer, Set<String>> nodes) {
        SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
        secondaryIndex.touchMultiple(nodes);
    }

    /**
     * remove all imprints that have not been touched since defined time interval.
     *
     * @return
     */
    public void removeOldImprintsAndElements(long timestamp) {
        SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
        Set<Integer> schemaElementIDsToBeRemoved = secondaryIndex.removeOldImprints(timestamp);
        schemaElementIDsToBeRemoved.forEach(schemaElementID -> deleteSchemaElement(schemaElementID));
    }


    /**
     * Return a specific vertex by property. Use only properties that are unique since only
     * the first vertex matching is returned.
     *
     * @param uniqueProperty
     * @param schemaHash
     * @return
     */
    public Vertex getVertexByHashID(String uniqueProperty, Integer schemaHash) {
        OrientGraph graph = factory.getTx();
        Iterator<Vertex> iterator = graph.getVertices(uniqueProperty, schemaHash).iterator();

        if (iterator.hasNext()) {
            Vertex vertex = iterator.next();
            graph.shutdown();
            return vertex;
        } else
            return null;

    }

    public Edge getEdgeByHashID(String uniqueProperty, Integer schemaHash) {
        OrientGraph graph = factory.getTx();
        Iterator<Edge> iterator = graph.getEdges(uniqueProperty, schemaHash).iterator();

        if (iterator.hasNext()) {
            Edge edge = iterator.next();
            graph.shutdown();
            return edge;
        } else
            return null;
    }
}
