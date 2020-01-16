package database;

import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import org.json.simple.JSONObject;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import scala.Serializable;
import schema.SchemaElement;

import java.io.File;
import java.util.*;

import static database.Constants.*;

/**
 * NOTE from Tinkerpop:  Edge := outVertex ---label---> inVertex.
 */
public class OrientDbOptwithMem implements Serializable {

    /************************************************
     defined once at start up for all dbs
     ************************************************/
    public static String URL = "plocal:localhost";
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

    public static void removeInstance(String database) {
        singletonInstances.remove(database);
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
            databaseServer.create(database, ODatabaseType.MEMORY);
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
        //    pool.close();
        //  databaseServer.close();
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

    private long timeSpentAdding = 0L;
    private long timeSpentDeleting = 0L;
    private long timeSpentReading = 0L;

    private Object addLock = new Object();
    private Object deleteLock = new Object();
    private Object readLock = new Object();


    public void resetTimes(){
        timeSpentAdding = 0L;
        timeSpentDeleting = 0L;
        timeSpentReading = 0L;
    }

    public void addTimeSpentAdding(long timeSpentAdding) {
        synchronized (addLock) {
            this.timeSpentAdding += timeSpentAdding;
        }
    }

    public void addTimeSpentDeleting(long timeSpentDeleting) {
        synchronized (deleteLock) {
            this.timeSpentDeleting += timeSpentDeleting;
        }
    }

    public void addTimeSpentReading(long timeSpentReading) {
        synchronized (readLock) {
            this.timeSpentReading += timeSpentReading;
        }
    }

    public long getTimeSpentAdding() {
        return timeSpentAdding;
    }

    public long getTimeSpentDeleting() {
        return timeSpentDeleting;
    }

    private OrientDbOptwithMem(String database, boolean trackChanges) {
        this.database = database;
        this.trackChanges = trackChanges;
        factory = new OrientGraphFactory(URL + "/" + database);
    }

    public void open() {
        factory = new OrientGraphFactory(URL + "/" + database);
    }


    public OrientGraphNoTx getGraph() {
        return factory.getNoTx();
    }


    /**
     * Checks whether a given vertex or edge exists, if the corresponding classString is provided.
     *
     * @param classString
     * @param hashValue
     * @return
     */
    public boolean exists(String classString, Integer hashValue) {

        long start = System.currentTimeMillis();
        boolean exists;
        if (classString == CLASS_SCHEMA_ELEMENT) {
            if(SecondaryIndexMem.getInstance() != null)
                exists =  SecondaryIndexMem.getInstance().checkSchemaElement(hashValue);
            else
                exists = getVertexByHashID(PROPERTY_SCHEMA_HASH, hashValue) != null;
        } else {
            System.err.println("Invalid exists-query!");
            return false;
        }
        addTimeSpentReading(System.currentTimeMillis() - start);
        return exists;
    }


    /**
     * CONVENTION:
     * the schema computation can return the following:
     * - no schema edges at all (k=0) => leads to no outgoing edges
     * - schema edges where we ignore the target (null) => leads to outgoing edges with an EMPTY TARGET placeholder
     * - schema edges where we take the schema of the neighbour into account
     * => write second-class schema element with no imprint vertex but shared among all first-class elements
     * <p>
     * <p>
     * This method writes the primary schema element as well as all required secondary schema elements.
     * It also updates the relationship from the schema element to the summarized instances (imprints)
     *
     * @param schemaElement
     */
    public void writeOrUpdateSchemaElement(SchemaElement schemaElement, Set<Integer> instances, boolean primary) {
        long start = System.currentTimeMillis();
        if (!exists(CLASS_SCHEMA_ELEMENT, schemaElement.getID())) {
            OrientGraphNoTx graph = getGraph();
            //create a new schema element
            Vertex vertex;
            try {
                Map<String, Object> properties = new HashMap<>();
                properties.put(PROPERTY_SCHEMA_HASH, schemaElement.getID());
                properties.put(PROPERTY_SCHEMA_VALUES, schemaElement.label());
                vertex = graph.addVertex("class:" + CLASS_SCHEMA_ELEMENT, properties);
                if (trackChanges && primary)
                    ChangeTracker.getInstance().incNewSchemaStructureObserved();
                if (trackChanges)
                    ChangeTracker.getInstance().incSchemaElementsAdded();
            } catch (ORecordDuplicatedException e) {
                //assumption, another thread has created it so ignore
                vertex = getVertexByHashID(PROPERTY_SCHEMA_HASH, schemaElement.getID());
            }

            //NOTE: the secondary index updates instance-schema-relations
            if (instances != null) {
                SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
                if (secondaryIndex != null)
                    secondaryIndex.putSummarizedInstances(schemaElement.getID(), instances);
            }
            for (Map.Entry<String, SchemaElement> entry : schemaElement.neighbors().entrySet()) {
                Integer endID = entry.getValue() == null ? EMPTY_SCHEMA_ELEMENT_HASH : entry.getValue().getID();
                Vertex targetV = getVertexByHashID(PROPERTY_SCHEMA_HASH, endID);
                if (targetV == null) {
                    //This node does not yet exist, so create one
                    //NOTE: neighbor elements are second-class citizens that exist as long as another schema element references them
                    //NOTE: this is a recursive step depending on chaining parameterization k
                    writeOrUpdateSchemaElement(entry.getValue() == null ? new SchemaElement() : entry.getValue(), null, false);
                }
                targetV = getVertexByHashID(PROPERTY_SCHEMA_HASH, endID);
                Edge edge = vertex.addEdge(CLASS_SCHEMA_RELATION, targetV);
                edge.setProperty(PROPERTY_SCHEMA_VALUES, entry.getKey());
            }
            graph.shutdown();
        } else {
            if (instances != null) {
                SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
                if (secondaryIndex != null)
                    secondaryIndex.addSummarizedInstances(schemaElement.getID(), instances);
            }
        }
        addTimeSpentAdding(System.currentTimeMillis() - start);
    }


    /**
     * After each write schema element, eventually, this method has to be called.
     * Updates the relationships from imprintIDs to actual imprints (creates them if new).
     *
     * @param nodes
     * @param schemaHash
     */
    public void addNodesToSchemaElement(Map<Integer, Set<String>> nodes, Integer schemaHash) {
        SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
        if (secondaryIndex != null)
            secondaryIndex.addNodesToSchemaElement(nodes, schemaHash);
    }


    /**
     * see removeNodeFromSchemaElement(Integer nodeID, Integer schemaHash)
     *
     * @param nodes
     */
    public void removeNodesFromSchemaElement(Map<Integer, Integer> nodes) {
        for (Map.Entry<Integer, Integer> node : nodes.entrySet())
            removeNodeFromSchemaElement(node.getKey(), node.getValue());
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
        if (secondaryIndex != null)
            return secondaryIndex.removeSummarizedInstance(schemaHash, nodeID);

        return false;
    }


    /**
     * If instances did not change their schema, still payload may has changed.
     * Anyways, the timestamp needs to be refreshed.
     *
     * @param nodes
     */
    public void touchMultiple(Map<Integer, Set<String>> nodes) {
        SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
        if (secondaryIndex != null)
            secondaryIndex.touchMultiple(nodes);
    }

    /**
     * Remove all imprints that have not been touched since defined time interval.
     *
     * @return
     */
    public void removeOldImprintsAndElements(long timestamp) {
        SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
        if (secondaryIndex != null) {
            Set<Integer> schemaElementIDsToBeRemoved = secondaryIndex.removeOldImprints(timestamp);
            bulkDeleteSchemaElements(schemaElementIDsToBeRemoved);
            if (trackChanges)
                ChangeTracker.getInstance().incSchemaStructureDeleted(schemaElementIDsToBeRemoved.size());
        }
    }


    ////Below, more like simple helper functions that do not change anything

    /**
     * Return a specific vertex by property. Use only properties that are unique since only
     * the first vertex matching is returned.
     *
     * @param uniqueProperty
     * @param schemaHash
     * @return
     */
    public Vertex getVertexByHashID(String uniqueProperty, Integer schemaHash) {
        long start = System.currentTimeMillis();
        OrientGraphNoTx graph = getGraph();
        Iterator<Vertex> iterator = graph.getVertices(uniqueProperty, schemaHash).iterator();
        if (iterator.hasNext()) {
            Vertex vertex = iterator.next();
            addTimeSpentReading(System.currentTimeMillis() - start);
            return vertex;
        } else{
            addTimeSpentReading(System.currentTimeMillis() - start);
            return null;
        }

    }


    /**
     * Get linked schema element hash from instance ID
     *
     * @param nodeID
     * @return
     */
    public Integer getPreviousElementID(Integer nodeID) {
        SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
        if (secondaryIndex != null)
            return secondaryIndex.getSchemaElementFromImprintID(nodeID);
        return null;
    }


    /**
     * This method returns a distinct set of payload entries for the given schema element id.
     *
     * @param schemaHash
     * @return
     */
    public Set<String> getPayloadOfSchemaElement(Integer schemaHash) {
        SecondaryIndexMem secondaryIndex = SecondaryIndexMem.getInstance();
        if (secondaryIndex != null)
            return secondaryIndex.getPayload(schemaHash);
        else
            return null;
    }

    /**
     * Closes connection to database
     */
    public void close() {
        factory.close();
        resetTimes();
    }


    /***************************************
     ********** Internal Methods ***********
     ***************************************/


    public void bulkDeleteSchemaElements(Set<Integer> schemaHashes) {
        long start = System.currentTimeMillis();
        OrientDB databaseServer = new OrientDB(URL, serverUser, serverPassword, OrientDBConfig.defaultConfig());
        ODatabasePool pool = new ODatabasePool(databaseServer, database, USERNAME, PASSWORD);
        try (ODatabaseSession databaseSession = pool.acquire()) {
            Integer[] schemaIDs = new Integer[schemaHashes.size()];
            schemaIDs = schemaHashes.toArray(schemaIDs);
            String script =
                    "BEGIN;" +
                            "FOREACH ($i IN " + Arrays.toString(schemaIDs) + "){\n" +
                            "  DELETE VERTEX " + CLASS_SCHEMA_ELEMENT + " WHERE " + PROPERTY_SCHEMA_HASH + " = $i;\n" +
                            "}" +
                            "COMMIT;";

            System.out.println("Bulk delete prepared in : " + (System.currentTimeMillis() - start) + "ms");
            OResultSet rs = databaseSession.execute("sql", script);
            rs.close();

            //for each schema element, check if removing it creates orphans (secondary schema elements with no primary attached)
            String statement = "DELETE VERTEX "+ CLASS_SCHEMA_ELEMENT + " WHERE both().size() = 0";
            OResultSet orphanResultSet = databaseSession.command(statement);
            JSONParser jsonParser = new JSONParser();

            long orphans = 0;
            while(orphanResultSet.hasNext()){
                OResult row = orphanResultSet.next();
                JSONObject jResult = (JSONObject) jsonParser.parse(row.toJSON());
                orphans = (long) jResult.get("count");

            }
            orphanResultSet.close();
            if (trackChanges) {
                ChangeTracker.getInstance().incSchemaElementsDeleted(schemaHashes.size());
                ChangeTracker.getInstance().incSchemaElementsDeleted((int) orphans); //TODO: should be no problem to cast
                ChangeTracker.getInstance().incSchemaStructureDeleted(schemaHashes.size());
            }
            timeSpentDeleting += (System.currentTimeMillis() - start);
            System.out.println("Bulk delete took: " + (System.currentTimeMillis() - start) + "ms");

        } catch (ParseException e) {
            e.printStackTrace();
        }

    }


    public long sizeOnDisk() {
//        ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:orientdb/databases/" + database);
//        db.open("admin", "admin");
//        OrientDB databaseServer = new OrientDB(URL, serverUser, serverPassword, OrientDBConfig.defaultConfig());
//        ODatabasePool pool = new ODatabasePool(databaseServer, database, USERNAME, PASSWORD);
//        try (ODatabaseSession databaseSession = pool.acquire()) {
//            OCommandOutputListener listener = iText -> {
//            }; //no log
//            ODatabaseExport export = new ODatabaseExport((ODatabaseDocumentInternal) databaseSession, "orientdb/exports/" + database + ".json.gz", listener);
//            export.exportDatabase();
//            export.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        File file = new File("orientdb/exports/" + database + ".json.gz");
//        return file.length();
        File dir = new File("orientdb/databases/" + database);
        long size = 0L;
        for (File file : dir.listFiles(F -> F.getName().contains("schema") | F.getName().startsWith("e_") | F.getName().startsWith("v_"))){
            // System.out.println(file.getName());
            size += file.length();
        }
        return size;
    }

    public long[] countSchemaElementsAndLinks() {
        OrientDB databaseServer = new OrientDB(URL, serverUser, serverPassword, OrientDBConfig.defaultConfig());
        ODatabasePool pool = new ODatabasePool(databaseServer, database, USERNAME, PASSWORD);
        long[] counts = new long[]{0, 0};
        try (ODatabaseSession databaseSession = pool.acquire()) {
            // Retrieve the User OClass
            OClass schemaElements = databaseSession.getClass(CLASS_SCHEMA_ELEMENT);
            counts[0] = schemaElements.count();
            OClass schemaLinks = databaseSession.getClass(CLASS_SCHEMA_RELATION);
            counts[1] = schemaLinks.count();
        }
        // Return User Count
        return counts;
    }

    public long getTimeSpentReading() {
        return timeSpentReading;
    }
}
