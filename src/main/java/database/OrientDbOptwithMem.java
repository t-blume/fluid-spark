package database;

import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import scala.Serializable;
import schema.SchemaElement;

import java.util.*;

import static database.Constants.*;

/**
 * NOTE from Tinkerpop:  Edge := outVertex ---label---> inVertex.
 */
public class OrientDbOptwithMem implements Serializable {

    /************************************************
     defined once at start up for all dbs
     ************************************************/
    public static String URL = "remote:localhost";
    public static String USERNAME = "admin";
    public static String PASSWORD = "admin";
    public static String serverUser = "root";
    public static String serverPassword = "rootpwd";
    /************************************************/

    private static HashMap<String, OrientDbOptwithMem> singletonInstances = null;

    public static OrientDbOptwithMem getInstance(String database, boolean trackChanges) {
        if (singletonInstances == null)
            singletonInstances = new HashMap<>();

        if (!singletonInstances.containsKey(database))
            singletonInstances.put(database, new OrientDbOptwithMem(database, trackChanges));

        return singletonInstances.get(database);
    }


    /**
     * Creates the database if not existed before.
     * Optionally, cleared the content.
     *
     * @param database
     * @param clear
     */
    public static void create(String database, boolean clear) {
        OrientDB databaseServer = new OrientDB(URL, serverUser, serverPassword, OrientDBConfig.defaultConfig());
        ODatabasePool pool = new ODatabasePool(databaseServer, database, USERNAME, PASSWORD);

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
//                databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createProperty(PROPERTY_SUMMARIZED_INSTANCES, OType.EMBEDDEDSET);

                /*
                Create relationships between schema elements
                 */
                databaseSession.createEdgeClass(CLASS_SCHEMA_RELATION);
                databaseSession.getClass(CLASS_SCHEMA_RELATION).createProperty(PROPERTY_SCHEMA_HASH, OType.INTEGER);
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
    private boolean trackChanges;
    //one connections object per database
    private OrientGraphFactory factory;


    private OrientDbOptwithMem(String database, boolean trackChanges) {
        this.database = database;
        this.trackChanges = trackChanges;
        factory = new OrientGraphFactory(URL + "/" + database).setupPool(1, 10);
    }

    public OrientGraph getGraph() {
        return factory.getTx();
    }


    /**
     * Checks whether a given vertex or edge exists, if the corresponding classString is provided.
     * @param classString
     * @param hashValue
     * @return
     */
    public boolean exists(String classString, Integer hashValue) {
        boolean exists;
        if (classString == CLASS_SCHEMA_ELEMENT) {
            exists = getVertexByHashID(PROPERTY_SCHEMA_HASH, hashValue) != null;
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
     *
     * This method writes the primary schema element as well as all required secondary schema elements.
     * It also updates the relationship from the schema element to the summarized instances (imprints)
     * @param schemaElement
     */
    public void writeOrUpdateSchemaElement(SchemaElement schemaElement, Set<Integer> instances, boolean primary) {
        OrientGraph graph = factory.getTx();
        if (!exists(CLASS_SCHEMA_ELEMENT, schemaElement.getID())) {
            if (trackChanges && primary)
                ChangeTracker.getInstance().incNewSchemaStructureObserved();

            //create a new schema element
            Vertex vertex = graph.addVertex("class:" + CLASS_SCHEMA_ELEMENT);
            vertex.setProperty(PROPERTY_SCHEMA_HASH, schemaElement.getID());
            vertex.setProperty(PROPERTY_SCHEMA_VALUES, schemaElement.label());

            //NOTE: the secondary index updates instance-schema-relations
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
            if (trackChanges)
                ChangeTracker.getInstance().incSchemaElementsAdded();

            graph.commit();
            graph.shutdown();
        } else {
            SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
            secondaryIndex.addSummarizedInstances(schemaElement.getID(), instances);
        }
    }


    /**
     * After each write schema element, eventually, this method has to be called.
     * Updates the relationships from imprintIDs to actual imprints (creates them if new).
     * @param nodes
     * @param schemaHash
     */
    public void addNodesToSchemaElement(Map<Integer, Set<String>> nodes, Integer schemaHash) {
        SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
        secondaryIndex.addNodesToSchemaElement(nodes, schemaHash);
    }




    /**
     * see removeNodeFromSchemaElement(Integer nodeID, Integer schemaHash)
     * @param nodes
     */
    public void removeNodesFromSchemaElement(Map<Integer, Integer> nodes) {
        for (Map.Entry<Integer, Integer> node : nodes.entrySet()) {
            removeNodeFromSchemaElement(node.getKey(), node.getValue());
        }
    }

    /**
     * After each write schema element, eventually, this method has to be called.
     * Returns true if it also removed the schema element
     *
     * @param nodeID
     * @param schemaHash
     * @return
     */
    public boolean removeNodeFromSchemaElement(Integer nodeID, Integer schemaHash) {
        SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
        boolean deleteSchemaElement = secondaryIndex.removeSummarizedInstance(schemaHash, nodeID);

        if (deleteSchemaElement) {
            deleteSchemaElement(schemaHash);
            //no more instance with that schema exists
            if (trackChanges)
                ChangeTracker.getInstance().incSchemaStructureDeleted();
        }
        return deleteSchemaElement;
    }


    /**
     * If instances did not change their schema, still payload may has changed.
     * Anyways, the timestamp needs to be refreshed.
     * @param nodes
     */
    public void touchMultiple(Map<Integer, Set<String>> nodes) {
        SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
        secondaryIndex.touchMultiple(nodes);
    }

    /**
     * Remove all imprints that have not been touched since defined time interval.
     *
     * @return
     */
    public void removeOldImprintsAndElements(long timestamp) {
        SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
        Set<Integer> schemaElementIDsToBeRemoved = secondaryIndex.removeOldImprints(timestamp);
        schemaElementIDsToBeRemoved.forEach(schemaElementID -> deleteSchemaElement(schemaElementID));
        if(trackChanges)
            ChangeTracker.getInstance().incSchemaStructureDeleted(schemaElementIDsToBeRemoved.size());
    }




    ////Below, more like simple helper functions that to not change anything

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


    /**
     * This method returns a distinct set of payload entries for the given schema element id.
     * @param schemaHash
     * @return
     */
    public Set<String> getPayloadOfSchemaElement(Integer schemaHash) {
        SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
        return secondaryIndex.getPayload(schemaHash);
    }

    /**
     * Closes connection to database
     */
    public void close() {
        //TODO save Memory Store here?
    }


    /***************************************
     ********** Internal Methods ***********
     ***************************************/

    /**
     * Deletes the schema element and all of its edges.
     * If the connected schema elements are no longer needed, delete as well.
     *
     * This method also updates the relationship from
     * @param schemaHash
     */
    private void deleteSchemaElement(Integer schemaHash) {
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

            //update secondary index
            SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
            Set<Integer> summarizedInstances = secondaryIndex.removeSchemaElement(v.getProperty(PROPERTY_SCHEMA_HASH));
            if (summarizedInstances != null)
                secondaryIndex.removeImprintLinksByID(summarizedInstances);


            graph.removeVertex(v);
            if (trackChanges)
                ChangeTracker.getInstance().incSchemaElementsDeleted();
        }
        graph.commit();
        graph.shutdown();
    }



}
