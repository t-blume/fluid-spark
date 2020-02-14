package database;

import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.graphx.EdgeTriplet;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import scala.Serializable;
import scala.Tuple2;
import scala.Tuple4;
import schema.SchemaElement;
import utils.MyHash;

import java.io.File;
import java.util.*;

import static database.Constants.*;

/**
 * NOTE from Tinkerpop:  Edge := outVertex ---label---> inVertex.
 */
public class OrientConnector implements Serializable {
    private static final Logger logger = LogManager.getLogger(OrientConnector.class.getSimpleName());


    /************************************************
     defined once at start up for all dbs
     ************************************************/
    public static String URL = "remote:localhost";
    public static String USERNAME = "admin";
    public static String PASSWORD = "admin";
    public static String serverUser = "root";
    public static String serverPassword = "rootpwd";
    /************************************************/

    private static HashMap<String, OrientConnector> singletonInstances = null;

    //construct that allows simultaneous connections to different databases
    public static OrientConnector getInstance(String database, boolean trackChanges, boolean trackExecutionTimes) {
        if (singletonInstances == null)
            singletonInstances = new HashMap<>();

        if (!singletonInstances.containsKey(database))
            singletonInstances.put(database, new OrientConnector(database, trackChanges, trackExecutionTimes));

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
                databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createProperty(PROPERTY_PAYLOAD, OType.EMBEDDEDSET);
//                databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createProperty(PROPERTY_SUMMARIZED_INSTANCES, OType.EMBEDDEDSET);

                /*
                Create relationships between schema elements
                 */
                databaseSession.createEdgeClass(CLASS_SCHEMA_RELATION);
                databaseSession.getClass(CLASS_SCHEMA_RELATION).createProperty(PROPERTY_SCHEMA_HASH, OType.INTEGER);
                databaseSession.getClass(CLASS_SCHEMA_RELATION).createIndex(CLASS_SCHEMA_RELATION + "." + PROPERTY_SCHEMA_HASH, OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, PROPERTY_SCHEMA_HASH);

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
    private final boolean trackChanges;
    //keep track of all update times
    private final boolean trackExecutionTimes;
    //one connections object per database
    private OrientGraphFactory factory;

    public SecondaryIndex getSecondaryIndex() {
        return secondaryIndex;
    }

    public void setSecondaryIndex(SecondaryIndex secondaryIndex) {
        this.secondaryIndex = secondaryIndex;
    }

    private SecondaryIndex secondaryIndex = null;

    public void deactivate() {
        secondaryIndex = null;
    }


    //orphans should be removed, except for class collections
    private boolean allowOrphans = false;

    public void setAllowOrphans(boolean allowOrphans) {
        this.allowOrphans = allowOrphans;
    }

    /**
     * @param database
     * @param trackChanges
     */
    private OrientConnector(String database, boolean trackChanges, boolean trackExecutionTimes) {
        this.database = database;
        this.trackChanges = trackChanges;
        this.trackExecutionTimes = trackExecutionTimes;
        factory = new OrientGraphFactory(URL + "/" + database);
    }

    /**
     * Create a connection factory for this specific OrientDB database
     */
    public void open() {
        factory = new OrientGraphFactory(URL + "/" + database);
    }

    /**
     * Get a OrientGraph (no transactional) from the factory.
     * the factory handles multiple connections to the same database.
     *
     * @return
     */
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
    public Result<Boolean> exists(String classString, Integer hashValue) {
        long start = System.currentTimeMillis();
        Result<Boolean> result = new Result<>(trackExecutionTimes, trackChanges);
        if (classString == CLASS_SCHEMA_ELEMENT) {
            if (secondaryIndex != null)
                result._result = secondaryIndex.checkSchemaElement(hashValue);
            else
                result._result = getVertexByHashID(PROPERTY_SCHEMA_HASH, hashValue)._result != null;
        } else {
            logger.error("Invalid exists-query!");
            result._result = false;
        }

        if (trackExecutionTimes)
            result._timeSpentReadingSecondaryIndex = System.currentTimeMillis() - start;

        return result;
    }


    public Result<Boolean> batchWrite(SchemaElement schemaElement) {
        HashSet<Integer> instanceIds = new HashSet();
        schemaElement.instances().forEach(i -> instanceIds.add(MyHash.md5HashString(i)));
        Result<Boolean> result = writeOrUpdateSchemaElement(schemaElement, instanceIds, true, true);
        return result;
    }


    public Result<Boolean> incrementalWrite(SchemaElement schemaElement) {
        System.out.println("Incremental write: " + schemaElement);
        HashSet<Integer> instanceIds = new HashSet();
        schemaElement.instances().forEach(i -> instanceIds.add(MyHash.md5HashString(i)));
        Result<Boolean> result = writeOrUpdateSchemaElement(schemaElement, instanceIds, true, false);

        // collect all Updates and perform them in a micro batch
        HashMap<Integer, Set<String>> nodesTobeAdded = new HashMap<>();
        HashMap<Integer, Set<String>> nodesTobeTouched = new HashMap<>();
        HashMap<Integer, Integer> nodesTobeRemoved = new HashMap<>();

        Iterator<String> instanceIterator = schemaElement.instances().iterator();
        while (instanceIterator.hasNext()) {
            String vertexID = instanceIterator.next();
            //check if previously known
            Result<Integer> preSchemaResult = getPreviousElementID(MyHash.md5HashString(vertexID));
            if (trackExecutionTimes)
                result.mergeTimes(preSchemaResult);

            Integer prevSchemaHash = preSchemaResult._result;
            if (prevSchemaHash != null) {
                //instance (vertex) was known before
                if (prevSchemaHash != schemaElement.getID()) {
                    //CASE: instance was known but with a different schema
                    // it was something else before, remove link to old schema element
                    if (trackChanges) {
                        Vertex prevSchemaElement = getVertexByHashID(PROPERTY_SCHEMA_HASH, prevSchemaHash)._result;
                        result._changeTracker.incInstancesWithChangedSchema();
                        //check if the schema would have been the same if no neighbor information was required
                        try {
                            if (prevSchemaElement != null && (schemaElement.label() == null &&
                                    prevSchemaElement.getProperty(Constants.PROPERTY_SCHEMA_VALUES) == null) || (
                                    schemaElement.label() != null && prevSchemaElement.getProperty(Constants.PROPERTY_SCHEMA_VALUES) != null &&
                                            schemaElement.label().hashCode() == prevSchemaElement.getProperty(Constants.PROPERTY_SCHEMA_VALUES).hashCode())) {
                                //the label sets are the same
                                Iterator<Edge> iter = prevSchemaElement.getEdges(Direction.OUT, Constants.CLASS_SCHEMA_RELATION).iterator();
                                HashSet<String> oldProperties = new HashSet();
                                while (iter.hasNext())
                                    oldProperties.add(iter.next().getProperty(Constants.PROPERTY_SCHEMA_VALUES));

                                Set<String> newProperties = schemaElement.neighbors().keySet();
                                //label are the same and properties are the same, so it must be a neighbor change
                                if (oldProperties.hashCode() == newProperties.hashCode())
                                    result._changeTracker.incInstancesChangedBecauseOfNeighbors();
                            }
                        } catch (NullPointerException ex) {
                            logger.error("WHAT THE FUCK?");
                        }
                    }
                    //also checks if old schema element is still needed, deleted otherwise
                    nodesTobeRemoved.put(MyHash.md5HashString(vertexID), prevSchemaHash);
                    //create link between instance/payload and schema
                    nodesTobeAdded.put(MyHash.md5HashString(vertexID), schemaElement.payload());//TODO Fix instance payload?
                } else {
                    //CASE: instance was known and the schema is the same
                    //update timestamp and optionally update payload if it is changed
                    nodesTobeTouched.put(MyHash.md5HashString(vertexID), schemaElement.payload());
//          println(MyHash.md5HashString(vertexID))
                }
            } else {
                //CASE: new instance added
                nodesTobeAdded.put(MyHash.md5HashString(vertexID), schemaElement.payload());
            }
        }
        Result addResult = addNodesToSchemaElement(nodesTobeAdded, schemaElement.getID());
        if (trackChanges || trackExecutionTimes)
            result.mergeAll(addResult);
        Result touchResult = touchMultiple(nodesTobeTouched);
        if (trackChanges || trackExecutionTimes)
            result.mergeAll(touchResult);
        Result deleteResult = removeNodesFromSchemaElement(nodesTobeRemoved, true);
        if (trackChanges || trackExecutionTimes)
            result.mergeAll(deleteResult);

        return result;
    }

    public Result<Boolean> updateCollection(final Collection edgeTriplets, boolean additions) {
        edgeTriplets.parallelStream().forEach(o -> {
            TripletWrapper tripletWrapper = (TripletWrapper) o;
            int imprintId = -1;
            String subjectURI = null;
            Set<String> labelSet = new HashSet<>();
            Set<String> payload = new HashSet<>();
            Iterator<EdgeTriplet<scala.collection.immutable.Set<Tuple2<String, String>>, Tuple4<String, String, String, String>>> iterator = tripletWrapper.triplets().iterator();
            while (iterator.hasNext()) {
                EdgeTriplet<scala.collection.immutable.Set<Tuple2<String, String>>, Tuple4<String, String, String, String>> triplet = iterator.next();
                payload.add(triplet.attr._4());
                if (imprintId == -1) {
                    imprintId = MyHash.md5HashString(triplet.attr._1());
                    subjectURI = triplet.attr._1();
                    if (triplet.srcAttr() != null) {
                        scala.collection.Iterator<Tuple2<String, String>> attrIterator = triplet.srcAttr().iterator();
                        while (attrIterator.hasNext()) {
                            Tuple2<String, String> attr = attrIterator.next();
                            payload.add(attr._2);
                            labelSet.add(attr._1);
                        }
                    }
                }
            }

            if (additions) {
                System.out.println("new payload: " + payload);
                secondaryIndex.addPayload(imprintId, payload);
                //TODO move this to index models
                System.out.println(imprintId);
                int schemaID = secondaryIndex.getSchemaElementFromImprintID(imprintId)._result;
                Vertex prevSchemaElement = getVertexByHashID(PROPERTY_SCHEMA_HASH, schemaID)._result;
                Set<String> prevLabels = prevSchemaElement.getProperty(PROPERTY_SCHEMA_VALUES);
                Set<String> newLabelSet = new HashSet<>();
                for (String label : labelSet) {
                    if (!prevLabels.contains(label))
                        newLabelSet.add(label);
                }
                if(newLabelSet.size() > 0){
                    SchemaElement schemaElement = new SchemaElement();
                    schemaElement.label().addAll(prevLabels);
                    schemaElement.label().addAll(newLabelSet);
                    schemaElement.instances().add(subjectURI);
                    System.out.println("New schema hash: " + schemaElement.getID());
                    incrementalWrite(schemaElement);
                }
            } else {
                secondaryIndex.removePayload(imprintId, payload);
            }
        });

        return null;
    }


    public Result<Boolean> writeCollection(final Collection schemaElements, boolean batch) {
        Result mainRes = new Result(trackExecutionTimes, trackChanges);
        if (trackChanges || trackExecutionTimes) {
//            List<Result> trackedResultList = (List<Result>) schemaElements.parallelStream().map(o -> incrementalWrite((SchemaElement) o)).collect(Collectors.toList());
//            trackedResultList.forEach(r -> mainRes.mergeAll(r));
            if (batch)
                mainRes = (Result<Boolean>) schemaElements.parallelStream()
                        .map(o -> batchWrite((SchemaElement) o))
                        .reduce((r1, r2) -> ((Result<Boolean>) r1).mergeAll((Result<Boolean>) r2)).get();
            else
                mainRes = (Result<Boolean>) schemaElements.parallelStream()
                        .map(o -> incrementalWrite((SchemaElement) o))
                        .reduce((r1, r2) -> ((Result<Boolean>) r1).mergeAll((Result<Boolean>) r2)).get();

        } else {
            if (batch)
                schemaElements.parallelStream().forEach(o -> batchWrite((SchemaElement) o));
            else
                schemaElements.parallelStream().forEach(o -> incrementalWrite((SchemaElement) o));
        }
        return mainRes;

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
    public Result<Boolean> writeOrUpdateSchemaElement(SchemaElement schemaElement, Set<Integer> instances,
                                                      boolean primary, boolean batch) {
        //check if already entry in primary index (actually check if entry in secondary index, cause faster)
        //monitor execution time?
        Result<Boolean> exists = exists(CLASS_SCHEMA_ELEMENT, schemaElement.getID());
        Result<Boolean> result = new Result<>(trackExecutionTimes, trackChanges);
        if (trackExecutionTimes)
            result.mergeTimes(exists);

        long start = System.currentTimeMillis();
        if (!exists._result) {
            OrientGraphNoTx graph = getGraph();
            //create a new schema element
            Vertex vertex;
            try {
                Map<String, Object> properties = new HashMap<>();
                properties.put(PROPERTY_SCHEMA_HASH, schemaElement.getID());
                properties.put(PROPERTY_SCHEMA_VALUES, schemaElement.label());
                if (batch)
                    properties.put(PROPERTY_PAYLOAD, schemaElement.payload());


                vertex = graph.addVertex("class:" + CLASS_SCHEMA_ELEMENT, properties);
                if (trackChanges) {
                    if (primary)
                        result._changeTracker.incNewSchemaStructureObserved();

                    result._changeTracker.incSchemaElementsAdded();
                }
                //created
                result._result = true;
            } catch (ORecordDuplicatedException e) {
                //Another thread has created it, thus, retrieve it
                vertex = getVertexByHashID(PROPERTY_SCHEMA_HASH, schemaElement.getID())._result;
                result._result = false;
            }
            if (trackExecutionTimes) {
                result._timeSpentReadingPrimaryIndex += (System.currentTimeMillis() - start);
            }
            //NOTE: the secondary index updates instance-schema-relations
            if (instances != null) {
                if (secondaryIndex != null) {
                    Result tmpResult = secondaryIndex.putSummarizedInstances(schemaElement.getID(), instances);
                    if (trackChanges || trackExecutionTimes)
                        result.mergeAll(tmpResult);
                }
            }
            if (schemaElement.neighbors() != null) {
                for (Map.Entry<String, SchemaElement> entry : schemaElement.neighbors().entrySet()) {
                    Integer endID = entry.getValue() == null ? EMPTY_SCHEMA_ELEMENT_HASH : entry.getValue().getID();
                    Result<Vertex> targetRes = getVertexByHashID(PROPERTY_SCHEMA_HASH, endID);
                    Vertex targetV = targetRes._result;
                    if (trackExecutionTimes)
                        result.mergeTimes(targetRes);
                    if (targetV == null) {
                        //This node does not yet exist, so create one
                        //NOTE: neighbor elements are second-class citizens that exist as long as another schema element references them
                        //NOTE: this is a recursive step depending on chaining parameterization k
                        Result<Boolean> tmpResult = writeOrUpdateSchemaElement(entry.getValue() == null ? new SchemaElement() : entry.getValue(), null, false, batch);
                        //sum all update operations and execution times
                        if (trackChanges || trackExecutionTimes)
                            result.mergeAll(tmpResult);

                        targetRes = getVertexByHashID(PROPERTY_SCHEMA_HASH, endID);
                        targetV = targetRes._result;
                    }
                    long t1 = System.currentTimeMillis();
                    //Edge edge = graph.addEdge(CLASS_SCHEMA_RELATION, vertex, targetV, CLASS_SCHEMA_RELATION);
                    //String.valueOf(MyHash.md5HashString(
                    //                        vertex.getId().toString() + entry.getKey() + targetV.getId().toString()))

                    Map<String, Object> properties = new HashMap<>();
                    properties.put(PROPERTY_SCHEMA_HASH, String.valueOf(MyHash.md5HashString(schemaElement.getID() + entry.getKey())));
                    properties.put(PROPERTY_SCHEMA_VALUES, entry.getKey());
                    try {
                        ((OrientVertex) vertex).addEdge(CLASS_SCHEMA_RELATION, (OrientVertex) targetV, new Object[]{properties});
                    } catch (ORecordDuplicatedException e) {
                        //Another thread has created edge already, thus, ignore edge
                    }
//                Edge edge = vertex.addEdge(CLASS_SCHEMA_RELATION, targetV);
//                edge.setProperty(PROPERTY_SCHEMA_VALUES, entry.getKey());
//                edge.setProperty(PROPERTY_SCHEMA_HASH, String.valueOf(MyHash.md5HashString(vertex.getId().toString() + entry.getKey() + targetV.getId().toString())));
//                graph.commit();
                    if (trackExecutionTimes)
                        result._timeSpentWritingSecondaryIndex += (System.currentTimeMillis() - t1);
                }
            }

            graph.shutdown();
        } else {
            System.out.println("exists?!");
            result._result = false;
//
            if (instances != null) {
                if (secondaryIndex != null) {
                    Result tmpRes = secondaryIndex.addSummarizedInstances(schemaElement.getID(), instances);
                    if (trackChanges || trackExecutionTimes)
                        result.mergeAll(tmpRes);
                }
            }
        }
        System.out.println("writeOrUpdateSchemaElement: " + result._result);
        return result;
    }


    /**
     * After each write schema element, eventually, this method has to be called.
     * Updates the relationships from imprintIDs to actual imprints (creates them if new).
     *
     * @param nodes
     * @param schemaHash
     */
    public Result<Boolean> addNodesToSchemaElement(Map<Integer, Set<String>> nodes, Integer schemaHash) {
        if (secondaryIndex != null)
            return secondaryIndex.addNodesToSchemaElement(nodes, schemaHash);
        else return new Result<>(trackExecutionTimes, trackChanges);
    }


    /**
     * see removeNodeFromSchemaElement(Integer nodeID, Integer schemaHash)
     *
     * @param nodes
     * @param lightDelete: imprints do not actually get deleted but are summarized by a different schema element
     *                     => count changes differently
     */
    public Result<Boolean> removeNodesFromSchemaElement(Map<Integer, Integer> nodes, boolean lightDelete) {
        Result<Boolean> result = new Result<>(trackExecutionTimes, trackChanges);
        for (Map.Entry<Integer, Integer> node : nodes.entrySet()) {
            Result<Boolean> tmpResult = removeNodeFromSchemaElement(node.getKey(), node.getValue(), lightDelete);
            if (trackChanges || trackExecutionTimes)
                result.mergeAll(tmpResult);
        }
        result._result = true;
        return result;
    }

    /**
     * After each write schema element, eventually, this method has to be called.
     * Returns true if it also removed the schema element
     *
     * @param nodeID
     * @param schemaHash
     * @param lightDelete: no payload change
     * @return
     */
    public Result<Boolean> removeNodeFromSchemaElement(Integer nodeID, Integer schemaHash, boolean lightDelete) {
        if (secondaryIndex != null)
            return secondaryIndex.removeSummarizedInstance(schemaHash, nodeID, lightDelete);
        else {
            Result<Boolean> result = new Result<>(trackExecutionTimes, trackChanges);
            result._result = false;
            return result;
        }
    }


    /**
     * If instances did not change their schema, still payload may has changed.
     * Anyways, the timestamp needs to be refreshed.
     *
     * @param nodes
     * @return
     */
    public Result<Boolean> touchMultiple(Map<Integer, Set<String>> nodes) {
        if (secondaryIndex != null)
            return secondaryIndex.touchMultiple(nodes);
        else {
            Result<Boolean> result = new Result<>(trackExecutionTimes, trackChanges);
            result._result = false;
            return result;
        }
    }

    /**
     * Remove all imprints that have not been touched since defined time interval.
     *
     * @return
     */
    public Result<Integer> removeOldImprintsAndElements(long timestamp) {
        Result<Integer> result = new Result<>(trackExecutionTimes, trackChanges);
        if (secondaryIndex != null) {
            Result<Set<Integer>> schemaElementIDsToBeRemoved = secondaryIndex.removeOldImprints(timestamp);
            Result<Boolean> tmpRes = bulkDeleteSchemaElements(schemaElementIDsToBeRemoved._result);
            if (trackChanges || trackExecutionTimes) {
                result.mergeAll(tmpRes);
                result.mergeAll(schemaElementIDsToBeRemoved);
            }
            result._result = schemaElementIDsToBeRemoved._result.size();
        } else
            result._result = 0;

        return result;
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
    public Result<Vertex> getVertexByHashID(String uniqueProperty, Integer schemaHash) {
        long start = System.currentTimeMillis();
        Result<Vertex> result = new Result<>(trackExecutionTimes, trackChanges);
        OrientGraphNoTx graph = getGraph();
        Iterator<Vertex> iterator = graph.getVertices(uniqueProperty, schemaHash).iterator();
        if (iterator.hasNext()) {
            Vertex vertex = iterator.next();
            if (trackExecutionTimes)
                result._timeSpentReadingPrimaryIndex = System.currentTimeMillis() - start;

            result._result = vertex;
            return result;
        } else {
            if (trackExecutionTimes)
                result._timeSpentReadingPrimaryIndex = System.currentTimeMillis() - start;

            return result;
        }
    }


    public Result<Edge> getEdgeByHashID(String uniqueProperty, Integer schemaHash) {
        long start = System.currentTimeMillis();
        Result<Edge> result = new Result<>(trackExecutionTimes, trackChanges);
        OrientGraphNoTx graph = getGraph();
        Iterator<Edge> iterator = graph.getEdges(uniqueProperty, schemaHash).iterator();
        if (iterator.hasNext()) {
            Edge edge = iterator.next();
            if (trackExecutionTimes)
                result._timeSpentReadingPrimaryIndex = System.currentTimeMillis() - start;

            result._result = edge;
            return result;
        } else {
            if (trackExecutionTimes)
                result._timeSpentReadingPrimaryIndex = System.currentTimeMillis() - start;

            return result;
        }
    }


    /**
     * Get linked schema element hash from instance ID
     *
     * @param nodeID
     * @return
     */
    public Result<Integer> getPreviousElementID(Integer nodeID) {
        if (secondaryIndex != null)
            return secondaryIndex.getSchemaElementFromImprintID(nodeID);
        else {
            Result<Integer> result = new Result<>(trackExecutionTimes, trackChanges);
            result._result = null;
            return result;
        }
    }


    /**
     * This method returns a distinct set of payload entries for the given schema element id.
     * ONLY USED for UNIt tests
     *
     * @param schemaHash
     * @return
     */
    public Set<String> getPayloadOfSchemaElement(Integer schemaHash) {
        if (secondaryIndex != null)
            return secondaryIndex.getPayload(schemaHash)._result;
        else {
            Result<Vertex> schemaElementResult = getVertexByHashID(PROPERTY_SCHEMA_HASH, schemaHash);
            if (schemaElementResult != null || schemaElementResult._result != null) {
                if (schemaElementResult._result.getProperty(Constants.PROPERTY_PAYLOAD) != null) {
                    return schemaElementResult._result.getProperty(Constants.PROPERTY_PAYLOAD);
                }
            }
        }
        return null;
    }

    /**
     * Closes connection to database
     */
    public void close() {
        factory.close();
    }


    /***************************************
     ********** Internal Methods ***********
     ***************************************/


    public Result<Boolean> bulkDeleteSchemaElements(Set<Integer> schemaHashes) {
        long start = System.currentTimeMillis();
        OrientDB databaseServer = new OrientDB(URL, serverUser, serverPassword, OrientDBConfig.defaultConfig());
        ODatabasePool pool = new ODatabasePool(databaseServer, database, USERNAME, PASSWORD);
        Result<Boolean> result = new Result<>(trackExecutionTimes, trackChanges);
        result._result = false;
        try (ODatabaseSession databaseSession = pool.acquire()) {
            Integer[] schemaIDs = new Integer[schemaHashes.size()];
            schemaIDs = schemaHashes.toArray(schemaIDs);
            String script =
                    "BEGIN;" +
                            "FOREACH ($i IN " + Arrays.toString(schemaIDs) + "){\n" +
                            "  DELETE VERTEX " + CLASS_SCHEMA_ELEMENT + " WHERE " + PROPERTY_SCHEMA_HASH + " = $i;\n" +
                            "}" +
                            "COMMIT;";

            logger.info("Bulk delete prepared in : " + (System.currentTimeMillis() - start) + "ms");
            OResultSet rs = databaseSession.execute("sql", script);
            rs.close();

            if (!allowOrphans) {
                //for each schema element, check if removing it creates orphans (secondary schema elements with no primary attached)
                String statement = "DELETE VERTEX " + CLASS_SCHEMA_ELEMENT + " WHERE both().size() = 0";
                OResultSet orphanResultSet = databaseSession.command(statement);
                JSONParser jsonParser = new JSONParser();

                long orphans = 0;
                logger.info("Counting delete results...");
                while (orphanResultSet.hasNext()) {
                    OResult row = orphanResultSet.next();
                    JSONObject jResult = (JSONObject) jsonParser.parse(row.toJSON());
                    orphans = (long) jResult.get("count");
                }
                orphanResultSet.close();
                logger.info("Bulk delete took: " + (System.currentTimeMillis() - start) + "ms");
                if (trackChanges)
                    result._changeTracker.incSchemaElementsDeleted((int) orphans); //TODO: should be no problem to cast
            }

            if (trackChanges) {
                result._changeTracker.incSchemaElementsDeleted(schemaHashes.size());
                result._changeTracker.incSchemaStructureDeleted(schemaHashes.size());
            }
            if (trackExecutionTimes)
                result._timeSpentDeletingPrimaryIndex = System.currentTimeMillis() - start;

            result._result = true;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        logger.info("Closing connection..");
        pool.close();
        return result;
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
        for (File file : dir.listFiles(F -> F.getName().contains("schema") | F.getName().startsWith("e_") | F.getName().startsWith("v_"))) {
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

}
