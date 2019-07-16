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
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import scala.Serializable;
import schema.SchemaElement;
import utils.MyHash;

import java.util.*;

import static database.Constants.*;

/**
 *
 * NOTE from Tinkerpop:  Edge := outVertex ---label---> inVertex.
 */
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

    private static Map<String, OrientDb> databaseConnections = new HashMap<>();

    private static OrientDB databaseServer = null;
    private static Map<String, ODatabasePool> pool = new HashMap<>();

    public static OrientDb getInstance(String database, boolean trackChanges) {
        if (!databaseConnections.containsKey(database)) {
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

            /*
                Create Schema Elements
             */
            databaseSession.createVertexClass(CLASS_SCHEMA_ELEMENT);
            databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createProperty(PROPERTY_SCHEMA_HASH, OType.INTEGER);
            databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createIndex(CLASS_SCHEMA_ELEMENT + "." + PROPERTY_SCHEMA_HASH, OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, PROPERTY_SCHEMA_HASH);
            databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createProperty(PROPERTY_SCHEMA_VALUES, OType.EMBEDDEDSET);
            //databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createProperty(PROPERTY_PAYLOAD, OType.EMBEDDEDSET);

            /*
            Create relationships between schema elements
             */
            databaseSession.createEdgeClass(CLASS_SCHEMA_RELATION);
            databaseSession.getClass(CLASS_SCHEMA_RELATION).createProperty(PROPERTY_SCHEMA_HASH, OType.INTEGER);

            /*
            Create super brain
             */
            databaseSession.createVertexClass(CLASS_IMPRINT_VERTEX);
            databaseSession.getClass(CLASS_IMPRINT_VERTEX).createProperty(PROPERTY_IMPRINT_ID, OType.INTEGER);
            databaseSession.getClass(CLASS_IMPRINT_VERTEX).createIndex(CLASS_IMPRINT_VERTEX + "." + PROPERTY_IMPRINT_ID, OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, PROPERTY_IMPRINT_ID);

            databaseSession.getClass(CLASS_IMPRINT_VERTEX).createProperty(PROPERTY_TIMESTAMP, OType.DATETIME);
            databaseSession.getClass(CLASS_IMPRINT_VERTEX).createProperty(PROPERTY_PAYLOAD, OType.EMBEDDEDSET);

            databaseSession.createEdgeClass(CLASS_IMPRINT_RELATION);
            databaseSession.getClass(CLASS_IMPRINT_RELATION).createProperty(PROPERTY_IMPRINT_ID, OType.INTEGER);
            databaseSession.getClass(CLASS_IMPRINT_RELATION).createIndex(CLASS_IMPRINT_RELATION + "." + PROPERTY_IMPRINT_ID, OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, PROPERTY_IMPRINT_ID);


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
        for (Vertex v : _graph.getVertices(PROPERTY_SCHEMA_HASH, schemaHash)) {
            Iterator<Edge> edgeIterator = v.getEdges(Direction.OUT, CLASS_SCHEMA_RELATION).iterator();
            while (edgeIterator.hasNext()){
                Edge edge = edgeIterator.next();
                Vertex linkedSchemaElement = edge.getVertex(Direction.IN);
                int remainingLinks = 0;
                Iterator<Edge> links = linkedSchemaElement.getEdges(Direction.IN).iterator();
                while (remainingLinks <= 2 && links.hasNext()){
                    links.next();
                    remainingLinks++;
                }
                if(remainingLinks < 2){
                    //when we remove this link, the linked schema element will be an orphan so remove it as well
                    deleteSchemaElement(linkedSchemaElement.getProperty(PROPERTY_SCHEMA_HASH));
                }

            }
            _graph.removeVertex(v);
            if (_changeTracker != null)
                _changeTracker._schemaElementsDeleted++;
        }

        _graph.commit();
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
    public void writeOrUpdateSchemaElement(SchemaElement schemaElement, boolean primary) {
        if (!exists(CLASS_SCHEMA_ELEMENT, schemaElement.getID())) {
            if(_changeTracker != null && primary)
                _changeTracker._newSchemaStructureObserved++;
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
                    writeOrUpdateSchemaElement(V == null ? new SchemaElement() : V, false);
                }
                targetV = getVertexByHashID(PROPERTY_SCHEMA_HASH, endID);
                Edge edge = _graph.addEdge("class:" + CLASS_SCHEMA_RELATION, vertex, targetV, CLASS_SCHEMA_RELATION);
                edge.setProperty(PROPERTY_SCHEMA_VALUES, K);
            });
            if (_changeTracker != null)
                _changeTracker._schemaElementsAdded++;

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

    public Edge getEdgeByHashID(String uniqueProperty, Integer schemaHash) {
        Iterator<Edge> iterator = _graph.getEdges(uniqueProperty, schemaHash).iterator();
        if (iterator.hasNext())
            return iterator.next();
        else
            return null;
    }


    public Set<String> getPayloadOfSchemaElement(Integer schemaHash) {
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

    /**
     * Returns true if it also removed the schema element
     *
     * @param nodeID
     * @param schemaHash
     * @return
     */
    public boolean removeNodeFromSchemaElement(Integer nodeID, Integer schemaHash) {
        //removes all edges between the imprint vertex and the schema vertex
        Iterator<Edge> edgeIterator = _graph.getEdges(CLASS_IMPRINT_RELATION + "." + PROPERTY_IMPRINT_ID, MyHash.md5HashImprintRelation(nodeID, schemaHash)).iterator();
        while (edgeIterator.hasNext()) {
            _graph.removeEdge(edgeIterator.next());
            if (_changeTracker != null)
                _changeTracker._removedInstanceToSchemaLinks++;
        }

        Iterator<Vertex> vertexIterator = _graph.getVertices(CLASS_SCHEMA_ELEMENT + "." + PROPERTY_SCHEMA_HASH, schemaHash).iterator();
        boolean removedSE = false;
        while (vertexIterator.hasNext()) {
            Iterator<Edge> innerIterator = vertexIterator.next().getEdges(Direction.IN, CLASS_IMPRINT_RELATION).iterator();
            if (!innerIterator.hasNext()) {
                deleteSchemaElement(schemaHash);
                removedSE = true;
                //no more instance with that schema exists
                if(_changeTracker != null)
                    _changeTracker._schemaStructureDeleted++;
            }
        }
        _graph.commit();
        return removedSE;
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

        //get the schema element
        iterator = _graph.getVertices(CLASS_SCHEMA_ELEMENT + "." + PROPERTY_SCHEMA_HASH, schemaHash).iterator();
        Vertex schema = iterator.next();


        if(!exists(CLASS_IMPRINT_RELATION,  MyHash.md5HashImprintRelation(nodeID, schemaHash))){
            Edge imprintEdge = _graph.addEdge("class:" + CLASS_IMPRINT_RELATION, imprint, schema, CLASS_IMPRINT_RELATION);
            imprintEdge.setProperty(PROPERTY_IMPRINT_ID, MyHash.md5HashImprintRelation(nodeID, schemaHash));
        }
        _graph.commit();

    }


    public void touch(Integer nodeID, Set<String> payload) {
        // get the imprint vertex
        Iterator<Vertex> iterator = _graph.getVertices(CLASS_IMPRINT_VERTEX + "." + PROPERTY_IMPRINT_ID, nodeID).iterator();
        Vertex imprint;
        if (!iterator.hasNext()) {
            System.err.println("This should not happen!");
        } else {
            imprint = iterator.next();
            imprint.setProperty(PROPERTY_TIMESTAMP, NOW());
            Set<String> oldPayload = imprint.getProperty(PROPERTY_PAYLOAD);
            if (oldPayload.hashCode() != payload.hashCode()) {
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
            OResult row = rs.next();
            for (int retry = 0; retry < MAX_RETRIES; ++retry) {
                try {
                    int nodeID = row.getProperty(PROPERTY_IMPRINT_ID);
                    Set<String> instancePayload = row.getProperty(PROPERTY_PAYLOAD);
                    if (_changeTracker != null) {
                        _changeTracker._payloadElementsChanged++;
                        _changeTracker._payloadEntriesRemoved += instancePayload.size();
                    }

                    Set<Vertex> linkedSchemaElements = new HashSet<>();
                    for (Vertex v : _graph.getVertices(PROPERTY_IMPRINT_ID, nodeID)) {
                        Iterator<Edge> edgeIterator = v.getEdges(Direction.OUT).iterator();
                        while (edgeIterator.hasNext()) {
                            Edge edge = edgeIterator.next();
                            Vertex linkedSchemaElement = edge.getVertex(Direction.OUT);
                            //update payload of this element
                            _graph.commit();
                            linkedSchemaElements.add(linkedSchemaElement);
                        }
                        _graph.removeVertex(v);
                        if (_changeTracker != null) {
                            _changeTracker._instancesDeleted++;
                        }
                    }
                    _graph.commit();
                    //iterate through all linked schema elements and check if there is still an instance linked to it
                    for (Vertex linkedSchemaElement : linkedSchemaElements) {
                        if (!linkedSchemaElement.getEdges(Direction.IN, CLASS_IMPRINT_RELATION).iterator().hasNext()) {
                            try {
                                _graph.removeVertex(linkedSchemaElement);
                                if (_changeTracker != null)
                                    _changeTracker._schemaElementsDeleted++;
                                _graph.commit();
                            } catch (ORecordNotFoundException e) {
                                //should be no problem since its already removed
                            }
                        }
                    }
                    if (_changeTracker != null)
                        _changeTracker._removedInstanceToSchemaLinks++;
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
                    _changeTracker._payloadElementsChanged++;
                    _changeTracker._payloadEntriesRemoved += deletions;
                }
                return oldPayload;
            } else {
                if (addOnly) {
                    int before = oldPayload.size();
                    oldPayload.addAll(newPayload);
                    int additions = oldPayload.size() - before;
                    if (additions > 0) {
                        _changeTracker._payloadElementsChanged++;
                        _changeTracker._payloadEntriesAdded += additions;
                    }
                    return oldPayload;
                } else {
                    int deletions = 0;
                    int additions = 0;
                    //pair-wise comparison needed
                    for (String oPay : oldPayload)
                        if (!newPayload.contains(oPay))
                            deletions++;

                    for (String nPay : newPayload)
                        if (!oldPayload.contains(nPay))
                            additions++;

                    if (additions > 0 || deletions > 0) {
                        _changeTracker._payloadElementsChanged++;
                        _changeTracker._payloadEntriesAdded += additions;
                        _changeTracker._payloadEntriesRemoved += deletions;
                    }
                    return newPayload;
                }
            }
        }
    }
}
