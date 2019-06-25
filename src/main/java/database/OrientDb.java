package database;

import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import scala.Serializable;
import schema.SchemaElement;
import utils.MyHash;

import java.util.*;

import static database.Constants.*;

public class OrientDb implements Serializable {

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

    private static Map<String,OrientDb> databaseConnections = new HashMap<>();

    private static OrientDB databaseServer = null;
    private static Map<String,ODatabasePool> pool = new HashMap<>();

    public static OrientDb getInstance(String database, boolean trackChanges) {
        if (!databaseConnections.containsKey(database)){
            databaseConnections.put(database, new OrientDb(new OrientGraph(URL + "/" + database), database, trackChanges));
        }
        return databaseConnections.get(database);
    }

    public static ODatabaseSession getDBSession(String database) {
        if (databaseServer == null) {
            databaseServer = new OrientDB(URL, "root", "rootpwd", OrientDBConfig.defaultConfig());
        }
        if (!pool.containsKey(database)) {
//            databaseServer.open(database, USERNAME, PASSWORD);

            pool.put(database, new ODatabasePool(databaseServer, database, "admin", "admin"));
        }
        // OPEN DATABASE
        try (ODatabaseSession db = pool.get(database).acquire()) {
            return db;
        }
    }

    /**
     * Creates the database if not existed before.
     * Optionally, cleared the content.
     *
     * @param database
     * @param clear
     */
    public static void create(String database, boolean clear) {
        databaseServer = new OrientDB(URL, serverUser, serverPassword, OrientDBConfig.defaultConfig());
        if (databaseServer.exists(database) && clear)
            databaseServer.drop(database);

        if (!databaseServer.exists(database)) {
            databaseServer.create(database, ODatabaseType.PLOCAL);

            ODatabaseSession databaseSession = databaseServer.open(database, USERNAME, PASSWORD);

            //this is quite important to align this with the OS
            databaseSession.command("ALTER DATABASE TIMEZONE \"GMT+2\"");

            databaseSession.createVertexClass(CLASS_SCHEMA_ELEMENT);
            databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createProperty(PROPERTY_SCHEMA_HASH, OType.INTEGER);
            databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createIndex(CLASS_SCHEMA_ELEMENT + "." + PROPERTY_SCHEMA_HASH, OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, PROPERTY_SCHEMA_HASH);
            databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createProperty(PROPERTY_SCHEMA_VALUES, OType.EMBEDDEDSET);
            //databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createProperty(PROPERTY_PAYLOAD, OType.EMBEDDEDSET);

            databaseSession.createEdgeClass(CLASS_SCHEMA_RELATION);
            databaseSession.getClass(CLASS_SCHEMA_RELATION).createProperty(PROPERTY_SCHEMA_HASH, OType.INTEGER);

            databaseSession.createVertexClass(CLASS_IMPRINT_VERTEX);
            databaseSession.getClass(CLASS_IMPRINT_VERTEX).createProperty(PROPERTY_IMPRINT_ID, OType.INTEGER);
            databaseSession.getClass(CLASS_IMPRINT_VERTEX).createIndex(CLASS_IMPRINT_VERTEX + "." + PROPERTY_IMPRINT_ID, OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, PROPERTY_IMPRINT_ID);

            databaseSession.getClass(CLASS_IMPRINT_VERTEX).createProperty(PROPERTY_TIMESTAMP, OType.DATETIME);
            databaseSession.getClass(CLASS_IMPRINT_VERTEX).createProperty(PROPERTY_PAYLOAD, OType.EMBEDDEDSET);

            databaseSession.createEdgeClass(CLASS_IMPRINT_RELATION);

            databaseSession.commit();
            databaseSession.close();
        }
    }


    /******************************************
     * Start of the OrientDB Connector object *
     ******************************************/

    //perform all graph operations on this object
    public OrientBaseGraph _graph;
    //name of the database
    private String database;
    //keep track of all update operations
    public ChangeTracker _changeTracker = null;


    public OrientDb(OrientBaseGraph graph, String database, boolean trackChanges) {
        _graph = graph;
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
        for (Vertex v : _graph.getVertices(PROPERTY_SCHEMA_HASH, schemaHash))
            _graph.removeVertex(v);

        _graph.commit();
    }


    public boolean exists(String classString, Integer schemaHash) {
        if (classString == CLASS_SCHEMA_ELEMENT) {
            boolean exists = getVertexByHashID(PROPERTY_SCHEMA_HASH, schemaHash) != null;
            return exists;
        } else if (classString == CLASS_IMPRINT_VERTEX) {
            boolean exists = getVertexByHashID(PROPERTY_IMPRINT_ID, schemaHash) != null;
            return exists;
        } else {
            System.err.println("Invalid exists-query!");
            return false;
        }
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
    public void writeOrUpdateSchemaElement(SchemaElement schemaElement) {
        if (!exists(CLASS_SCHEMA_ELEMENT, schemaElement.getID())) {
            //create a new schema element
            Vertex vertex = _graph.addVertex("class:" + CLASS_SCHEMA_ELEMENT);
            vertex.setProperty(PROPERTY_SCHEMA_HASH, schemaElement.getID());
            vertex.setProperty(PROPERTY_SCHEMA_VALUES, schemaElement.label());

            schemaElement.neighbors().forEach((K, V) -> {
                // if schema computation did not set a neighbor element, then use an empty one
                Integer endID = V == null ? EMPTY_SCHEMA_ELEMENT_HASH : V.getID();
                Vertex targetV = getVertexByHashID(PROPERTY_SCHEMA_HASH, endID);
                if (targetV == null) {
                    //This node does not yet exist, so create one
                    //NOTE: neighbor elements are second-class citizens that exist as long as another schema element references them
                    //NOTE: this is a recursive step depending on chaining parameterization k
                    writeOrUpdateSchemaElement(V == null ? new SchemaElement() : V);
                }
                targetV = getVertexByHashID(PROPERTY_SCHEMA_HASH, endID);
                Edge edge = _graph.addEdge("class:" + CLASS_SCHEMA_RELATION, vertex, targetV, CLASS_SCHEMA_RELATION);
                edge.setProperty(PROPERTY_SCHEMA_VALUES, K);
            });
            if (_changeTracker != null)
                _changeTracker.getSchemaElementsAddedThisIteration().add(schemaElement.getID());

//            //payload
//            if (schemaElement.payload().size() > 0) {
//                vertex.setProperty(PROPERTY_PAYLOAD, schemaElement.payload());
//                if (_changeTracker != null) {
//                    _changeTracker.incPayloadElementsChangedThisIteration();
//                    _changeTracker.incPayloadEntriesAdded(schemaElement.payload().size());
//                }
//                _graph.commit();
//            }
        } else {
//            //only update payload of existing schema element
//            Vertex vertex = getVertexByHashID(PROPERTY_SCHEMA_HASH, schemaElement.getID());
//            Set<String> payload = vertex.getProperty(PROPERTY_PAYLOAD);
//            int changes = 0;
//            for (String pay : schemaElement.payload()) {
//                if (!payload.contains(pay)) {
//                    payload.add(pay);
//                    changes++;
//                }
//            }
//            if (changes > 0) {
//                vertex.setProperty(PROPERTY_PAYLOAD, payload);
//                if (_changeTracker != null) {
//                    _changeTracker.incPayloadElementsChangedThisIteration();
//                    _changeTracker.incPayloadEntriesAdded(changes);
//                }
//                _graph.commit();
//            }
        }
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
        Iterator<Vertex> iterator = _graph.getVertices(uniqueProperty, schemaHash).iterator();
        if (iterator.hasNext())
            return iterator.next();
        else
            return null;
    }


    public Set<String> getPayloadOfSchemaElement(Integer schemaHash){
        Iterator<Vertex> iterator = _graph.getVertices(CLASS_SCHEMA_ELEMENT + "." + PROPERTY_SCHEMA_HASH, schemaHash).iterator();
        if (iterator.hasNext()) {
            Iterator<Edge> innerIterator = iterator.next().getEdges(Direction.IN, CLASS_IMPRINT_RELATION).iterator();
            Set<String> payload = new HashSet<>();
            while (innerIterator.hasNext())
                payload.addAll(innerIterator.next().getVertex(Direction.OUT).getProperty(PROPERTY_PAYLOAD));
            return payload;
        }
        return null;
    }


    /**
     * Closes conenction to database
     */
    public void close() {
        _graph.shutdown();
    }


    /**
     * Get linked schema element hash from instance ID
     *
     * @param nodeID
     * @return
     */
    public Integer getPreviousElementID(Integer nodeID) {
        Iterator<Vertex> iterator = _graph.getVertices(CLASS_IMPRINT_VERTEX + "." + PROPERTY_IMPRINT_ID, nodeID).iterator();
        if (iterator.hasNext()) {
            Iterator<Edge> innerIterator = iterator.next().getEdges(Direction.OUT, CLASS_IMPRINT_RELATION).iterator();
            if (innerIterator.hasNext())
                return innerIterator.next().getVertex(Direction.IN).getProperty(PROPERTY_SCHEMA_HASH);
        }
        return null;
    }

    /**
     * Get linked schema element from instance ID
     *
     * @param nodeID
     * @return
     */
    public Vertex getPreviousElement(Integer nodeID) {
        Iterator<Vertex> iterator = _graph.getVertices(CLASS_IMPRINT_VERTEX + "." + PROPERTY_IMPRINT_ID, nodeID).iterator();
        if (iterator.hasNext()) {
            Iterator<Edge> innerIterator = iterator.next().getEdges(Direction.OUT, CLASS_IMPRINT_RELATION).iterator();
            if (innerIterator.hasNext())
                return innerIterator.next().getVertex(Direction.IN);
        }
        return null;
    }

    public Integer removeNodeFromSchemaElement(Integer nodeID, Integer schemaHash) {
        StringBuilder sb = new StringBuilder();
        sb.append(nodeID);
        sb.append(schemaHash);

        //removes all edges between the imprint vertex and the schema vertex
        Iterator<Edge> iterator = _graph.getEdges(CLASS_IMPRINT_RELATION + "." + PROPERTY_IMPRINT_ID, sb.toString().hashCode()).iterator();
        while (iterator.hasNext())
            _graph.removeEdge(iterator.next());
        //TODO: remove schema element here and maybe the imprint vertex

        _graph.commit();
        return 1;
    }



    /**
     * Update or create an imprint vertex and link it to the schema element and the payload
     *
     * @param nodeID
     * @param schemaHash
     * @param payload
     * @return
     */
    public void addNodeToSchemaElement(Integer nodeID, Integer schemaHash, Set<String> payload) {
        // get or create the imprint vertex
        Iterator<Vertex> iterator = _graph.getVertices(CLASS_IMPRINT_VERTEX + "." + PROPERTY_IMPRINT_ID, nodeID).iterator();
        Vertex imprint;
        if (!iterator.hasNext()) {
            imprint = _graph.addVertex("class:" + CLASS_IMPRINT_VERTEX);
            //unique reference to instance
            imprint.setProperty(PROPERTY_IMPRINT_ID, nodeID);
            //memorize which payload was extracted from this instance
            imprint.setProperty(PROPERTY_PAYLOAD, updatePayload(new HashSet<>(), payload, true, false));
            //set current time so avoid deletion after completion
            imprint.setProperty(PROPERTY_TIMESTAMP, NOW());
        } else {
            imprint = iterator.next();
            Set<String> oldPayload = imprint.getProperty(PROPERTY_PAYLOAD);
            //handle payload updates here
            if (oldPayload.hashCode() != payload.hashCode()) {
                //the payload extracted from that instance has changed
                //set the payload to to exactly new new one
                imprint.setProperty(PROPERTY_PAYLOAD, updatePayload(oldPayload, payload, false, false));
                _graph.commit();
            }
            //set current time so avoid deletion after completion
            imprint.setProperty(PROPERTY_TIMESTAMP, NOW());
        }

        iterator = _graph.getVertices(CLASS_SCHEMA_ELEMENT + "." + PROPERTY_SCHEMA_HASH, schemaHash).iterator();
        Vertex schema = iterator.next();

        StringBuilder sb = new StringBuilder();
        sb.append(nodeID);
        sb.append(schemaHash);
        int edgeID = MyHash.md5HashString(sb.toString());

        Edge imprintEdge = _graph.addEdge(edgeID, imprint, schema, CLASS_IMPRINT_RELATION);
        imprintEdge.setProperty(PROPERTY_IMPRINT_ID, edgeID);
        _graph.commit();

    }


    public void touch(Integer nodeID, Set<String> payload) {
        // get the imprint vertex
        Iterator<Vertex> iterator = _graph.getVertices(CLASS_IMPRINT_VERTEX + "." + PROPERTY_IMPRINT_ID, nodeID).iterator();
        Vertex imprint;
        if (!iterator.hasNext()) {
            System.err.println("This should not happen!");
//            imprint = _graph.addVertex("class:" + CLASS_IMPRINT_VERTEX);
//            imprint.setProperty(PROPERTY_IMPRINT_ID, nodeID);
//            imprint.setProperty(PROPERTY_TIMESTAMP, NOW());
        } else {
            imprint = iterator.next();
            imprint.setProperty(PROPERTY_TIMESTAMP, NOW());
            Set<String> oldPayload = imprint.getProperty(PROPERTY_PAYLOAD);
            if(oldPayload.hashCode() != payload.hashCode()){
                imprint.setProperty(PROPERTY_PAYLOAD, updatePayload(oldPayload, payload, false, false));
            }
        }
        _graph.commit();
    }

    /**
     * remove all imprints that have not been touched since defined time interval.
     * returns the number of deleted imprints
     *
     * @return
     */
    public int removeOldImprintsAndElements(String timestamp) {
        ODatabaseSession session = databaseServer.open(database, USERNAME, PASSWORD);
        String statement = "select * from ImprintVertex  where timestamp < ?";
        OResultSet rs = session.query(statement, timestamp);
        int i = 0;
        while (rs.hasNext()) {
            for (int retry = 0; retry < MAX_RETRIES; ++retry) {
                try {
                    OResult row = rs.next();
                    int nodeID = row.getProperty(PROPERTY_IMPRINT_ID);
//                    Set<String> instancePayload = row.getProperty(PROPERTY_PAYLOAD);
                    Set<Vertex> linkedSchemaElements = new HashSet<>();
                    for (Vertex v : _graph.getVertices(PROPERTY_IMPRINT_ID, nodeID)) {
                        Iterator<Edge> edgeIterator = v.getEdges(Direction.OUT).iterator();
                        while (edgeIterator.hasNext()) {
                            Edge edge = edgeIterator.next();
                            Vertex linkedSchemaElement = edge.getVertex(Direction.OUT);
                            //update payload of this element
//                            linkedSchemaElement.setProperty(PROPERTY_PAYLOAD, updatePayload(linkedSchemaElement.getProperty(PROPERTY_PAYLOAD), instancePayload, false));
                            _graph.commit();
                            linkedSchemaElements.add(linkedSchemaElement);
                        }
                        _graph.removeVertex(v);
                    }
                    _graph.commit();
                    //iterate through all linked schema elements and check if there is still an instance linked to it
                    for (Vertex linkedSchemaElement : linkedSchemaElements) {
                        if (!linkedSchemaElement.getEdges(Direction.IN, CLASS_IMPRINT_RELATION).iterator().hasNext()) {
                            try {
                                _graph.removeVertex(linkedSchemaElement);
                                if (_changeTracker != null)
                                    _changeTracker.getSchemaElementsDeletedThisIteration().add(linkedSchemaElement.getProperty(PROPERTY_SCHEMA_HASH));
                                _graph.commit();
                            } catch (ORecordNotFoundException e) {
                                //should be no problem since its already removed
                            }
                        }
                    }
                    if (_changeTracker != null)
                        _changeTracker.incRemovedInstancesSchemaLinks();
                    _graph.commit();
                    System.out.println(row);
                    i++;
                    break;
                } catch (OConcurrentModificationException e) {
                    System.out.println("Retry " + retry);
                }
            }

        }
        rs.close();
        return i;
    }









    private Set<String> updatePayload(Set<String> oldPayload, Set<String> newPayload, boolean addOnly, boolean removeOnly) {
        if (_changeTracker == null) {
            if (addOnly) {
                oldPayload.addAll(newPayload);
                return oldPayload;
            } else if (removeOnly) {
                oldPayload.removeAll(newPayload);
                return oldPayload;
            } else
                return newPayload;
        } else {
            if (removeOnly) {
                int before = oldPayload.size();
                oldPayload.removeAll(newPayload);
                int deletions = before - oldPayload.size();
                if (deletions > 0) {
                    _changeTracker.incPayloadElementsChangedThisIteration();
                    _changeTracker.incPayloadEntriesRemoved(deletions);
                }
                return oldPayload;
            } else {
                int before = oldPayload.size();
                oldPayload.addAll(newPayload);
                int additions = oldPayload.size() - before;
                if (addOnly) {
                    if (additions > 0) {
                        _changeTracker.incPayloadElementsChangedThisIteration();
                        _changeTracker.incPayloadEntriesAdded(additions);
                    }
                    return oldPayload;
                } else {
                    int deletions = oldPayload.size() - newPayload.size();
                    if (additions > 0 || deletions > 0)
                        _changeTracker.incPayloadElementsChangedThisIteration();
                    _changeTracker.incPayloadEntriesRemoved(deletions);
                    return newPayload;
                }
            }
        }
    }


}
